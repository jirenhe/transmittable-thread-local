package com.alibaba.ttl.threadpool.agent.internal.transformlet.impl;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.alibaba.ttl.threadpool.TtlExecutors;
import com.alibaba.ttl.threadpool.agent.internal.logging.Logger;
import com.alibaba.ttl.threadpool.agent.internal.transformlet.ClassInfo;
import com.alibaba.ttl.threadpool.agent.internal.transformlet.JavassistTransformlet;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtMethod;
import javassist.NotFoundException;
import org.jetbrains.annotations.NotNull;

import static com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.Utils.isClassAtPackageJavaUtil;
import static com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.Utils.signatureOfMethod;

/**
 * TTL {@link JavassistTransformlet} for {@link java.util.concurrent.Executor}.
 *
 * @author Jerry Lee (oldratlee at gmail dot com)
 * @author wuwen5 (wuwen.55 at aliyun dot com)
 * @see java.util.concurrent.Executor
 * @see java.util.concurrent.ExecutorService
 * @see java.util.concurrent.ThreadPoolExecutor
 * @see java.util.concurrent.ScheduledThreadPoolExecutor
 * @see java.util.concurrent.Executors
 * @see TtlPriorityBlockingQueueTransformlet
 * @since 2.5.1
 */
public class TtlExecutorTransformlet implements JavassistTransformlet {
    private static final Logger logger = Logger.getLogger(TtlExecutorTransformlet.class);

    private static final Map<String, String> PARAM_TYPE_NAME_TO_DECORATE_METHOD_CLASS = new HashMap<>();

    private static final String RUNNABLE_CLASS_NAME = "java.lang.Runnable";

    /**
     * Only apply executor instrumentation to allowed executors. To apply to all executors, use
     * override setting above.
     */
    private static final Collection<String> includeExecutors = new HashSet<>();

    /**
     * Some frameworks have their executors defined as anon classes inside other classes. Referencing
     * anon classes by name would be fragile, so instead we will use list of class prefix names. Since
     * checking this list is more expensive (O(n)) we should try to keep it short.
     */
    private static final Collection<String> includePrefixes = new HashSet<>();

    static {
        //这里的逻辑是从io.opentelemetry.javaagent.instrumentation.executors.AbstractExecutorInstrumentation的代码抄过来的
        String[] includeExecutors = {"akka.actor.ActorSystemImpl$$anon$1", "akka.dispatch.BalancingDispatcher",
            "akka.dispatch.Dispatcher", "akka.dispatch.Dispatcher$LazyExecutorServiceDelegate",
            "akka.dispatch.ExecutionContexts$sameThreadExecutionContext$", "akka.dispatch.forkjoin.ForkJoinPool",
            "akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinPool", "akka.dispatch.MessageDispatcher",
            "akka.dispatch.PinnedDispatcher", "com.google.common.util.concurrent.AbstractListeningExecutorService",
            "com.google.common.util.concurrent.MoreExecutors$ListeningDecorator",
            "com.google.common.util.concurrent.MoreExecutors$ScheduledListeningDecorator",
            "io.netty.channel.epoll.EpollEventLoop", "io.netty.channel.epoll.EpollEventLoopGroup",
            "io.netty.channel.MultithreadEventLoopGroup", "io.netty.channel.nio.NioEventLoop",
            "io.netty.channel.nio.NioEventLoopGroup", "io.netty.channel.SingleThreadEventLoop",
            "io.netty.util.concurrent.AbstractEventExecutor", "io.netty.util.concurrent.AbstractEventExecutorGroup",
            "io.netty.util.concurrent.AbstractScheduledEventExecutor", "io.netty.util.concurrent.DefaultEventExecutor",
            "io.netty.util.concurrent.DefaultEventExecutorGroup", "io.netty.util.concurrent.GlobalEventExecutor",
            "io.netty.util.concurrent.MultithreadEventExecutorGroup",
            "io.netty.util.concurrent.SingleThreadEventExecutor", "java.util.concurrent.AbstractExecutorService",
            "java.util.concurrent.CompletableFuture$ThreadPerTaskExecutor",
            "java.util.concurrent.Executors$DelegatedExecutorService",
            "java.util.concurrent.Executors$FinalizableDelegatedExecutorService", "java.util.concurrent.ForkJoinPool",
            "java.util.concurrent.ScheduledThreadPoolExecutor", "java.util.concurrent.ThreadPoolExecutor",
            "org.apache.tomcat.util.threads.ThreadPoolExecutor", "org.eclipse.jetty.util.thread.QueuedThreadPool",
            // dispatch() covered in the jetty module
            "org.eclipse.jetty.util.thread.ReservedThreadExecutor",
            "org.glassfish.grizzly.threadpool.GrizzlyExecutorService", "org.jboss.threads.EnhancedQueueExecutor",
            "play.api.libs.streams.Execution$trampoline$",
            "play.shaded.ahc.io.netty.util.concurrent.ThreadPerTaskExecutor", "scala.concurrent.forkjoin.ForkJoinPool",
            "scala.concurrent.Future$InternalCallbackExecutor$", "scala.concurrent.impl.ExecutionContextImpl",};
        Set<String> combined = new HashSet<>(Arrays.asList(includeExecutors));
        String include = System.getProperty("ttl.instrumentation.executors.include");
        if (include != null) {
            String[] split = include.split(",");
            combined.addAll(Arrays.asList(split));
        }
        TtlExecutorTransformlet.includeExecutors.addAll(combined);

        String[] includePrefixes = {"slick.util.AsyncExecutor$"};
        TtlExecutorTransformlet.includePrefixes.addAll(Arrays.asList(includePrefixes));

        PARAM_TYPE_NAME_TO_DECORATE_METHOD_CLASS.put(RUNNABLE_CLASS_NAME, "com.alibaba.ttl.TtlRunnable");
        PARAM_TYPE_NAME_TO_DECORATE_METHOD_CLASS.put("java.util.concurrent.Callable", "com.alibaba.ttl.TtlCallable");
    }

    private static final String THREAD_FACTORY_CLASS_NAME = "java.util.concurrent.ThreadFactory";

    private final boolean disableInheritableForThreadPool;

    public TtlExecutorTransformlet(boolean disableInheritableForThreadPool) {
        this.disableInheritableForThreadPool = disableInheritableForThreadPool;
    }

    @Override
    public void doTransform(@NonNull final ClassInfo classInfo)
        throws IOException, NotFoundException, CannotCompileException {
        // work-around ClassCircularityError:
        //      https://github.com/alibaba/transmittable-thread-local/issues/278
        //      https://github.com/alibaba/transmittable-thread-local/issues/234
        String className = classInfo.getClassName();
        if (isClassAtPackageJavaUtil(className)) {return;}

        final CtClass clazz = classInfo.getCtClass();
        if (includeExecutors.contains(className)) {
            tryTransform(classInfo, clazz);
        } else {
            for (String includePrefix : includePrefixes) {
                if (className.startsWith(includePrefix)) {
                    tryTransform(classInfo, clazz);
                }
            }
        }
    }

    private void tryTransform(@NotNull ClassInfo classInfo, CtClass clazz)
        throws NotFoundException, CannotCompileException {
        for (CtMethod method : clazz.getDeclaredMethods()) {
            updateSubmitMethodsOfExecutorClass_decorateToTtlWrapperAndSetAutoWrapperAttachment(method);
        }

        if (disableInheritableForThreadPool) {updateConstructorDisableInheritable(clazz);}

        classInfo.setModified();
    }

    /**
     * @see com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.Utils#doAutoWrap(Runnable)
     * @see com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.Utils#doAutoWrap(Callable)
     */
    @SuppressFBWarnings("VA_FORMAT_STRING_USES_NEWLINE") // [ERROR] Format string should use %n rather than \n
    private void updateSubmitMethodsOfExecutorClass_decorateToTtlWrapperAndSetAutoWrapperAttachment(
        @NonNull final CtMethod method) throws NotFoundException, CannotCompileException {
        final int modifiers = method.getModifiers();
        if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) {return;}

        CtClass[] parameterTypes = method.getParameterTypes();
        StringBuilder insertCode = new StringBuilder();
        for (int i = 0; i < parameterTypes.length; i++) {
            final String paramTypeName = parameterTypes[i].getName();
            if (PARAM_TYPE_NAME_TO_DECORATE_METHOD_CLASS.containsKey(paramTypeName)) {
                String code = String.format(
                    // auto decorate to TTL wrapper
                    "$%d = com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.Utils.doAutoWrap($%<d);", i + 1);
                insertCode.append(code);
            }
        }
        if (insertCode.length() > 0) {
            logger.info(
                "insert code before method " + signatureOfMethod(method) + " of class " + method.getDeclaringClass()
                    .getName() + ":\n" + insertCode);
            method.insertBefore(insertCode.toString());
        }
    }

    /**
     * @see TtlExecutors#getDisableInheritableThreadFactory(java.util.concurrent.ThreadFactory)
     */
    private void updateConstructorDisableInheritable(@NonNull final CtClass clazz)
        throws NotFoundException, CannotCompileException {
        for (CtConstructor constructor : clazz.getDeclaredConstructors()) {
            final CtClass[] parameterTypes = constructor.getParameterTypes();
            final StringBuilder insertCode = new StringBuilder();
            for (int i = 0; i < parameterTypes.length; i++) {
                final String paramTypeName = parameterTypes[i].getName();
                if (THREAD_FACTORY_CLASS_NAME.equals(paramTypeName)) {
                    String code = String.format(
                        "$%d = com.alibaba.ttl.threadpool.TtlExecutors.getDisableInheritableThreadFactory($%<d);",
                        i + 1);
                    insertCode.append(code);
                }
            }
            if (insertCode.length() > 0) {
                logger.info("insert code before constructor " + signatureOfMethod(constructor) + " of class "
                    + constructor.getDeclaringClass().getName() + ": " + insertCode);
                constructor.insertBefore(insertCode.toString());
            }
        }
    }

    /**
     * @see Utils#doUnwrapIfIsAutoWrapper(Runnable)
     */
    private boolean updateBeforeAndAfterExecuteMethodOfExecutorSubclass(@NonNull final CtClass clazz)
        throws NotFoundException, CannotCompileException {
        final CtClass runnableClass = clazz.getClassPool().get(RUNNABLE_CLASS_NAME);
        final CtClass threadClass = clazz.getClassPool().get("java.lang.Thread");
        final CtClass throwableClass = clazz.getClassPool().get("java.lang.Throwable");
        boolean modified = false;

        try {
            final CtMethod beforeExecute = clazz.getDeclaredMethod("beforeExecute",
                new CtClass[] {threadClass, runnableClass});
            // unwrap runnable if IsAutoWrapper
            String code
                = "$2 = com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.Utils.doUnwrapIfIsAutoWrapper($2);";
            logger.info("insert code before method " + signatureOfMethod(beforeExecute) + " of class "
                + beforeExecute.getDeclaringClass().getName() + ": " + code);
            beforeExecute.insertBefore(code);
            modified = true;
        } catch (NotFoundException e) {
            // clazz does not override beforeExecute method, do nothing.
        }

        try {
            final CtMethod afterExecute = clazz.getDeclaredMethod("afterExecute",
                new CtClass[] {runnableClass, throwableClass});
            // unwrap runnable if IsAutoWrapper
            String code
                = "$1 = com.alibaba.ttl.threadpool.agent.internal.transformlet.impl.Utils.doUnwrapIfIsAutoWrapper($1);";
            logger.info("insert code before method " + signatureOfMethod(afterExecute) + " of class "
                + afterExecute.getDeclaringClass().getName() + ": " + code);
            afterExecute.insertBefore(code);
            modified = true;
        } catch (NotFoundException e) {
            // clazz does not override afterExecute method, do nothing.
        }

        return modified;
    }
}
