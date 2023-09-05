package com.xxl.job.core.server;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.impl.ExecutorBizImpl;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.thread.ExecutorRegistryThread;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.ThrowableUtil;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * <1>执行器这一段内嵌的 Netty 服务器</1>
 *
 * @author xuxueli 2020-04-11 21:25
 */
public class EmbedServer {
    private static final Logger logger = LoggerFactory.getLogger(EmbedServer.class);

    /**
     * 执行器接口，在 start 方法中会被接口的实现类赋值
     */
    private ExecutorBiz executorBiz;
    /**
     * 启动 Netty 服务器的线程，这说明内嵌服务器的启动也是异步的
     */
    private Thread thread;

    /**
     * <h2>启动执行器的内嵌服务器</h2>
     */
    public void start(final String address, final int port, final String appname, final String accessToken) {
        // 给 executorBiz 赋值，这个 ExecutorBizImpl 对象相当重要，它就是用来执行定时任务的
        executorBiz = new ExecutorBizImpl();
        // 创建线程，在线程中启动 Netty 服务器
        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                // param
                EventLoopGroup bossGroup = new NioEventLoopGroup();
                EventLoopGroup workerGroup = new NioEventLoopGroup();
                // bizThreadPool 线程池会传入到下面的 EmbedHttpServerHandler 入站处理器中
                ThreadPoolExecutor bizThreadPool = new ThreadPoolExecutor(
                        0,
                        200,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>(2000),
                        new ThreadFactory() {
                            @Override
                            public Thread newThread(Runnable r) {
                                return new Thread(r, "xxl-job, EmbedServer bizThreadPool-" + r.hashCode());
                            }
                        },
                        new RejectedExecutionHandler() {
                            @Override
                            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                                throw new RuntimeException("xxl-job, EmbedServer bizThreadPool is EXHAUSTED!");
                            }
                        });
                try {
                    // start server
                    ServerBootstrap bootstrap = new ServerBootstrap();
                    bootstrap.group(bossGroup, workerGroup)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel channel) throws Exception {
                                    channel.pipeline()
                                            // 心跳检测
                                            .addLast(new IdleStateHandler(0, 0, 30 * 3, TimeUnit.SECONDS))  // beat 3N, close if idle
                                            // HTTP 编解码器，该处理器既是入站处理器，也是出站处理器
                                            .addLast(new HttpServerCodec())
                                            // 聚合消息，当传递的 HTTP 消息过大时会被拆开，这里添加这个处理器就是把拆分的消息再次聚合起来，形成一个整体再向后传递
                                            // 该处理器是入站处理器
                                            .addLast(new HttpObjectAggregator(5 * 1024 * 1024))  // merge request & reponse to FULL
                                            // 添加入站处理器，在该处理器中执行定时任务
                                            .addLast(new EmbedHttpServerHandler(executorBiz, accessToken, bizThreadPool));
                                }
                            })
                            .childOption(ChannelOption.SO_KEEPALIVE, true);
                    // 绑定端口号
                    ChannelFuture future = bootstrap.bind(port).sync();
                    logger.info(">>>>>>>>>>> xxl-job remoting server start success, nettype = {}, port = {}", EmbedServer.class, port);
                    // 注册执行器到调度中心
                    startRegistry(appname, address);
                    // 等待关闭
                    future.channel().closeFuture().sync();
                } catch (InterruptedException e) {
                    logger.info(">>>>>>>>>>> xxl-job remoting server stop.");
                } catch (Exception e) {
                    logger.error(">>>>>>>>>>> xxl-job remoting server error.", e);
                } finally {
                    // 优雅释放资源
                    try {
                        workerGroup.shutdownGracefully();
                        bossGroup.shutdownGracefully();
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
        thread.setDaemon(true);    // daemon, service jvm, user thread leave >>> daemon leave >>> jvm leave
        thread.start();
    }

    /**
     * <h2>销毁资源</h2>
     */
    public void stop() throws Exception {
        // destroy server thread
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
        }

        // 销毁注册执行器到调度中心的线程
        stopRegistry();
        logger.info(">>>>>>>>>>> xxl-job remoting server destroy success.");
    }


    // ---------------------- registry ----------------------

    /**
     * <h2>程序内部定义的入站处理器，这个处理器会进行定时任务方法的调用</h2>
     */
    public static class EmbedHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private static final Logger logger = LoggerFactory.getLogger(EmbedHttpServerHandler.class);

        /**
         * 很重要的对象，其实就是 ExecutorBizImpl，该对象调用定时方法
         */
        private ExecutorBiz executorBiz;
        /**
         * TOKEN 令牌
         */
        private String accessToken;
        /**
         * bizThreadPool 会赋值给下面的属性
         */
        private ThreadPoolExecutor bizThreadPool;

        public EmbedHttpServerHandler(ExecutorBiz executorBiz, String accessToken, ThreadPoolExecutor bizThreadPool) {
            this.executorBiz = executorBiz;
            this.accessToken = accessToken;
            this.bizThreadPool = bizThreadPool;
        }

        /**
         * <h2>入站方法，在该方法中进行定时任务的调用</h2>
         */
        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
            // 获取发送过来的消息
            String requestData = msg.content().toString(CharsetUtil.UTF_8);
            // 获得 URI，这个 URI 就是调度中心访问 Netty 服务器时的 URI，这里之所以要获取它，是因为 URI 中有调度中心访问服务器的具体请求
            String uri = msg.uri();
            // 发送请求的方法
            HttpMethod httpMethod = msg.method();
            // 判断 HTTP 连接是否还存活
            boolean keepAlive = HttpUtil.isKeepAlive(msg);
            // 从请求中获取 TOKEN
            String accessTokenReq = msg.headers().get(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN);

            // 上面 Netty 的单线程执行器为我们解析了消息，下面的工作就应该交给用户定义的工作线程来执行
            // 否则会拖垮 Netty 的单线程执行器处理 IO 事件的效率
            bizThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    // 调度中心触发的定时任务，得到返回结果
                    Object responseObj = process(httpMethod, uri, requestData, accessTokenReq);
                    // 序列化
                    String responseJson = GsonTool.toJson(responseObj);
                    // 把消息会写给调度中心，注意：这里回复消息的动作是业务线程发起的
                    // 真正发送消息还是由单线程执行器来完成的
                    writeResponse(ctx, keepAlive, responseJson);
                }
            });
        }

        /**
         * <h3>核心处理方法</h3>
         */
        private Object process(HttpMethod httpMethod, String uri, String requestData, String accessTokenReq) {
            // HttpMethod 是否为 POST
            if (HttpMethod.POST != httpMethod) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
            }
            // URI 是否为空
            if (uri == null || uri.trim().length() == 0) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
            }
            // 判断执行器令牌是否和调度中心令牌一样，这里也能发现，调度中心和执行器的 TOKEN 令牌一定要是相等的，因为判断是双向的，两边都要判断
            if (accessToken != null
                    && accessToken.trim().length() > 0
                    && !accessToken.equals(accessTokenReq)) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
            }

            // services mapping
            try {
                // 开始从 URI 中具体判断，调度中心触发的是什么任务
                switch (uri) {
                    case "/beat":
                        // 这里触发的是心跳检测，判断执行器这一端是否启动了
                        return executorBiz.beat();
                    case "/idleBeat":
                        // 这里就是判断调度中心要调度的任务是否可以顺利执行，其实就是判断该任务是否正在被
                        // 执行器这一端执行或者在执行器的队列中，如果在的话，说明当前执行器比较繁忙
                        IdleBeatParam idleBeatParam = GsonTool.fromJson(requestData, IdleBeatParam.class);
                        return executorBiz.idleBeat(idleBeatParam);
                    case "/run":
                        // run 就意味着是要执行定时任务
                        // 把 requestData 转化成触发器参数对象，也就是 TriggerParam 对象
                        TriggerParam triggerParam = GsonTool.fromJson(requestData, TriggerParam.class);
                        // 然后交给 ExecutorBizImpl 对象去执行定时任务
                        return executorBiz.run(triggerParam);
                    case "/kill":
                        // 走到这个分支就意味着要终止任务
                        KillParam killParam = GsonTool.fromJson(requestData, KillParam.class);
                        return executorBiz.kill(killParam);
                    case "/log":
                        // 远程访问执行器端日志
                        LogParam logParam = GsonTool.fromJson(requestData, LogParam.class);
                        return executorBiz.log(logParam);
                    default:
                        return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + uri + ") not found.");
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, "request error:" + ThrowableUtil.toString(e));
            }
        }

        /**
         * <h3>把执行的定时任务结果发送到调度中心</h3>
         */
        private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, String responseJson) {
            // 设置响应结果
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(responseJson, CharsetUtil.UTF_8));   //  Unpooled.wrappedBuffer(responseJson)
            // 设置文本类型
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html;charset=UTF-8");       // HttpHeaderValues.TEXT_PLAIN.toString()
            // 设置消息字节的长度
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            if (keepAlive) {
                // 连接是存活状态
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
            // 开始发送消息
            ctx.writeAndFlush(response);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error(">>>>>>>>>>> xxl-job provider netty_http server caught exception", cause);
            ctx.close();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                ctx.channel().close();      // beat 3N, close if idle
                logger.debug(">>>>>>>>>>> xxl-job provider netty_http server close an idle channel.");
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    // ---------------------- registry ----------------------

    /**
     * <h2>启动注册线程，然后把定时器注册到调度中心</h2>
     */
    public void startRegistry(final String appname, final String address) {
        // start registry
        ExecutorRegistryThread.getInstance().start(appname, address);
    }

    /**
     * <h2>销毁注册线程</h2>
     */
    public void stopRegistry() {
        // stop registry
        ExecutorRegistryThread.getInstance().toStop();
    }

}
