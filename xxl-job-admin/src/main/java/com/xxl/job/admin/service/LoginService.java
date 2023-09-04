package com.xxl.job.admin.service;

import com.xxl.job.admin.core.model.XxlJobUser;
import com.xxl.job.admin.core.util.CookieUtil;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.admin.core.util.JacksonUtil;
import com.xxl.job.admin.dao.XxlJobUserDao;
import com.xxl.job.core.biz.model.ReturnT;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.DigestUtils;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.math.BigInteger;

/**
 * <h1>用户登录核心业务</h1>
 *
 * @author xuxueli 2019-05-04 22:13:264
 */
@Configuration
public class LoginService {

    /**
     * 用户登录身份
     */
    public static final String LOGIN_IDENTITY_KEY = "XXL_JOB_LOGIN_IDENTITY";

    @Resource
    private XxlJobUserDao xxlJobUserDao;

    /**
     * <h2>生成 TOKEN</h2>
     */
    private String makeToken(XxlJobUser xxlJobUser){
        // 把用户个人信息转换为字符创
        String tokenJson = JacksonUtil.writeValueAsString(xxlJobUser);
        // 把当前字符串转换成 16 进制的字符串
        String tokenHex = new BigInteger(tokenJson.getBytes()).toString(16);
        return tokenHex;
    }

    /**
     * <h2>解析 TOKEN</h2>
     */
    private XxlJobUser parseToken(String tokenHex){
        XxlJobUser xxlJobUser = null;
        if (tokenHex != null) {
            String tokenJson = new String(new BigInteger(tokenHex, 16).toByteArray());      // username_password(md5)
            xxlJobUser = JacksonUtil.readValue(tokenJson, XxlJobUser.class);
        }
        return xxlJobUser;
    }

    /**
     * <h2>登录逻辑</h2>
     */
    public ReturnT<String> login(HttpServletRequest request, HttpServletResponse response, String username, String password, boolean ifRemember){

        // param
        if (username==null || username.trim().length()==0 || password==null || password.trim().length()==0){
            return new ReturnT<String>(500, I18nUtil.getString("login_param_empty"));
        }

        // valid passowrd
        XxlJobUser xxlJobUser = xxlJobUserDao.loadByUserName(username);
        if (xxlJobUser == null) {
            return new ReturnT<String>(500, I18nUtil.getString("login_param_unvalid"));
        }
        String passwordMd5 = DigestUtils.md5DigestAsHex(password.getBytes());
        if (!passwordMd5.equals(xxlJobUser.getPassword())) {
            return new ReturnT<String>(500, I18nUtil.getString("login_param_unvalid"));
        }

        String loginToken = makeToken(xxlJobUser);

        // do login
        CookieUtil.set(response, LOGIN_IDENTITY_KEY, loginToken, ifRemember);
        return ReturnT.SUCCESS;
    }

    /**
     * <h2>登出</h2>
     */
    public ReturnT<String> logout(HttpServletRequest request, HttpServletResponse response){
        CookieUtil.remove(request, response, LOGIN_IDENTITY_KEY);
        return ReturnT.SUCCESS;
    }

    /**
     * <h2>是否登录</h2>
     */
    public XxlJobUser ifLogin(HttpServletRequest request, HttpServletResponse response){
        String cookieToken = CookieUtil.getValue(request, LOGIN_IDENTITY_KEY);
        if (cookieToken != null) {
            XxlJobUser cookieUser = null;
            try {
                cookieUser = parseToken(cookieToken);
            } catch (Exception e) {
                logout(request, response);
            }
            if (cookieUser != null) {
                XxlJobUser dbUser = xxlJobUserDao.loadByUserName(cookieUser.getUsername());
                if (dbUser != null) {
                    if (cookieUser.getPassword().equals(dbUser.getPassword())) {
                        return dbUser;
                    }
                }
            }
        }
        return null;
    }


}
