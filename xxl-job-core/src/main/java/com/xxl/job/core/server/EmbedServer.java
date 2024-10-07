package com.xxl.job.core.server;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.impl.ExecutorBizImpl;
import com.xxl.job.core.biz.model.IdleBeatParam;
import com.xxl.job.core.biz.model.KillParam;
import com.xxl.job.core.biz.model.LogParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.thread.ExecutorRegistryThread;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.ThrowableUtil;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 执行器这一端内嵌的Netty服务器。
 */
@Slf4j
public class EmbedServer {

     // 执行器接口，在start方法中会被接口的实现类赋值
    private ExecutorBiz executorBiz;
     // 启动Netty服务器的线程，这说明内嵌服务器的启动也是异步的
    private Thread thread;

    /**
     * 启动执行器的内嵌Netty服务器
     *
     * @param address     执行器的IP地址
     * @param port        执行器的端口号
     * @param appname     执行器配置文件中用户设定的执行器的唯一标识
     * @param accessToken 配置文件中用户设定的访问token
     */
    public void start(final String address, final int port, final String appname, final String accessToken) {
        // 给executorBiz赋值，它就是用来执行定时任务的
        executorBiz = new ExecutorBizImpl();

        Runnable executorBizStarter = () -> {
            // bizThreadPool线程池会传入到下面的EmbedHttpServerHandler入站处理器中
            ThreadPoolExecutor bizThreadPool = new ThreadPoolExecutor(
                    0,
                    200,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(2000),
                    r -> new Thread(r, "xxl-job, EmbedServer bizThreadPool-" + r.hashCode()),
                    (r, executor) -> {
                        throw new RuntimeException("xxl-job, EmbedServer bizThreadPool is EXHAUSTED!");
                    }
            );

            EventLoopGroup bossGroup = new NioEventLoopGroup();
            EventLoopGroup workerGroup = new NioEventLoopGroup();

            try {
                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childOption(ChannelOption.SO_KEEPALIVE, true)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel channel) throws Exception {
                                channel.pipeline()
                                        // 心跳检测
                                        .addLast(new IdleStateHandler(0, 0, 30 * 3, TimeUnit.SECONDS))
                                        // HTTP编解码器，duplex混合型事件处理器，既能处理入站事件又能处理出站事件
                                        .addLast(new HttpServerCodec())
                                        /*
                                        聚合消息，当传递的HTTP消息过大时会被拆开。
                                        这里添加这个处理器就是把拆分的消息再次聚合
                                        起来，形成一个整体再向后传递，该处理器是入
                                        站处理器。
                                         */
                                        .addLast(new HttpObjectAggregator(5 * 1024 * 1024))
                                        // 添加入站处理器，在该处理器中执行定时任务
                                        .addLast(new EmbedHttpServerHandler(executorBiz, accessToken, bizThreadPool));
                            }
                        });

                ChannelFuture future = bootstrap.bind(port).sync();
                log.info(">>>>>>>>>>> xxl-job remoting server start success, nettype = {}, port = {}", EmbedServer.class, port);
                // ==执行器注册到调度中心==
                startRegistry(appname, address);
                future.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                log.info(">>>>>>>>>>> xxl-job remoting server stop.");
            } catch (Exception e) {
                log.error(">>>>>>>>>>> xxl-job remoting server error.", e);
            } finally {
                try {
                    workerGroup.shutdownGracefully();
                    bossGroup.shutdownGracefully();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        };

        thread = new Thread(executorBizStarter);
        thread.setDaemon(true);
        thread.start();
    }


    // ====== registry ======

    /**
     * 执行器端入站处理器。
     * 当调度中心执行任务调度时，请求就会走到这里来。
     */
    @AllArgsConstructor
    public static class EmbedHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private ExecutorBiz executorBiz;
        private String accessToken;
        private ThreadPoolExecutor bizThreadPool;

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
            // 获取发送过来的请求数据
            String requestData = msg.content().toString(CharsetUtil.UTF_8);
            // 获取调度中心访问Netty服务器时的URI
            String uri = msg.uri();

            // 获取HttpMethod
            HttpMethod httpMethod = msg.method();
            // 判断HTTP连接是否还存活
            boolean keepAlive = HttpUtil.isKeepAlive(msg);

            // 从请求头中获取TOKEN
            String accessTokenReq = msg.headers().get(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN);

            /*
            上面Netty的单线程执行器为我们解析了消息，下面的工作就应该交给用户定义的工作
            线程来执行，否则会拖垮Netty的单线程执行器，从而影响其处理IO事件的效率。
             */
            bizThreadPool.execute(() -> {
                // ==调度中心触发定时任务==
                Object responseObj = process(httpMethod, uri, requestData, accessTokenReq);
                // 把定时任务执行的结果进行序列化
                String responseJson = GsonTool.toJson(responseObj);
                /*
                把响应写回给调度中心，注意：这里回复消息的动作是业务线程发起的，
                但真正发送消息还是由Netty的EventLoop单线程执行器来完成的。
                 */
                writeResponse(ctx, keepAlive, responseJson);
            });
        }

        private Object process(HttpMethod httpMethod, String uri, String requestData, String accessTokenReq) {
            // 判断是不是POST方法，因为调度中心发送消息时就是使用的post请求发送的
            if (HttpMethod.POST != httpMethod) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
            }

            // 判断URI是否为空
            if (uri == null || uri.trim().length() == 0) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
            }

            // 判断执行器令牌是否和调度中心令牌一样，这里也能发现，调度中心和执行器的TOKEN令牌一定要是相等的
            if (accessToken != null && accessToken.trim().length() > 0 && !accessToken.equals(accessTokenReq)) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
            }

            try {
                // 通过URI判断，调度中心触发的是什么任务，也就是到底调用哪个
                switch (uri) {
                    // 心跳检测
                    case "/beat":
                        // 调度中心检测执行器是否在线时使用
                        return executorBiz.beat();
                    // 忙碌检测
                    case "/idleBeat":
                        /*
                        这里就是判断调度中心要调度的任务是否可以顺利执行，其实就是判断该任务是否正在被
                        执行器这一端执行或者在执行器的队列中，如果在的话，说明当前执行器比较繁忙
                         */
                        IdleBeatParam idleBeatParam = GsonTool.fromJson(requestData, IdleBeatParam.class);
                        return executorBiz.idleBeat(idleBeatParam);
                    // 触发任务
                    case "/run":
                        TriggerParam triggerParam = GsonTool.fromJson(requestData, TriggerParam.class);
                        return executorBiz.run(triggerParam);
                    // 终止任务
                    case "/kill":
                        KillParam killParam = GsonTool.fromJson(requestData, KillParam.class);
                        return executorBiz.kill(killParam);
                    // 查看任务执行日志
                    case "/log":
                        LogParam logParam = GsonTool.fromJson(requestData, LogParam.class);
                        return executorBiz.log(logParam);
                    default:
                        return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + uri + ") not found.");
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "request error:" + ThrowableUtil.toString(e));
            }
        }

        private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, String responseJson) {
            // 设置响应结果
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(responseJson, CharsetUtil.UTF_8));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html;charset=UTF-8");
            // 设置消息字节的长度
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            // 连接是存活状态
            if (keepAlive) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            // 将定时任务执行结果写回给调度中心
            ctx.writeAndFlush(response);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error(">>>>>>>>>>> xxl-job provider netty_http server caught exception", cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.channel().close();
                log.debug(">>>>>>>>>>> xxl-job provider netty_http server close an idle channel.");
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }


    /**
     * 启动注册线程，然后把定时器注册到调度中心
     */
    public void startRegistry(final String appname, final String address) {
        ExecutorRegistryThread.getInstance().start(appname, address);
    }

    /**
     * 销毁资源
     */
    public void stop() throws Exception {
        // destroy server thread
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }

        // 销毁注册执行器到调度中心的线程
        stopRegistry();
        log.info(">>>>>>>>>>> xxl-job remoting server destroy success.");
    }

    public void stopRegistry() {
        ExecutorRegistryThread.getInstance().toStop();
    }
}
