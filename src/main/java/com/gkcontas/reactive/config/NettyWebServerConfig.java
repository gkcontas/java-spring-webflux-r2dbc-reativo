package com.gkcontas.reactive.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.embedded.netty.NettyReactiveWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Forces the reactive stack onto Netty.
 * <p>
 * This bean exists because of a silent default. {@code spring-boot-starter-web}
 * is on the classpath for the blocking half of the comparison, and it brings
 * embedded Tomcat with it. Tomcat can serve a reactive application too, through
 * a servlet bridge, and Spring Boot's auto-configuration prefers it over Netty
 * when both are available. So an application that asks for
 * {@code web-application-type: reactive} still starts a {@code TomcatWebServer}
 * - WebFlux on a servlet container, with a 200-thread pool in front of it and
 * no event loop in sight.
 * <p>
 * Nothing breaks. The endpoints answer, the types are all {@code Mono} and
 * {@code Flux}, and every measurement about thread economy is quietly wrong.
 * Declaring the factory here wins because the auto-configuration is
 * {@code @ConditionalOnMissingBean(ReactiveWebServerFactory.class)}.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
public class NettyWebServerConfig {

    @Bean
    public NettyReactiveWebServerFactory nettyReactiveWebServerFactory() {
        return new NettyReactiveWebServerFactory();
    }
}
