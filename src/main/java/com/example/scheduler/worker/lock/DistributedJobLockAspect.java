package com.example.scheduler.worker.lock;

import com.example.scheduler.common.Exceptions.JobOverlapException;
import java.util.concurrent.TimeUnit;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

/**
 * Overlap guard. Redisson locks are thread-bound: the annotated method must run on the worker
 * thread that executes the job. The lock is a liveness optimisation; correctness is enforced by
 * the unique execution claim and lease_version fencing in the database.
 */
@Aspect
@Component
public class DistributedJobLockAspect {

  private final RedissonClient redisson;
  private final ExpressionParser parser = new SpelExpressionParser();
  private final DefaultParameterNameDiscoverer names = new DefaultParameterNameDiscoverer();

  public DistributedJobLockAspect(RedissonClient redisson) { this.redisson = redisson; }

  @Around("@annotation(lock)")
  public Object around(ProceedingJoinPoint pjp, DistributedJobLock lock) throws Throwable {
    MethodSignature sig = (MethodSignature) pjp.getSignature();
    StandardEvaluationContext ctx = new StandardEvaluationContext();
    String[] pn = names.getParameterNames(sig.getMethod());
    for (int i = 0; pn != null && i < pn.length; i++) ctx.setVariable(pn[i], pjp.getArgs()[i]);
    Object k = parser.parseExpression(lock.key()).getValue(ctx);
    String key = "joblock:" + k;

    RLock rl = redisson.getLock(key);
    boolean ok = lock.leaseSeconds() > 0
        ? rl.tryLock(lock.waitMs(), lock.leaseSeconds(), TimeUnit.SECONDS)
        : rl.tryLock(lock.waitMs(), TimeUnit.MILLISECONDS);
    if (!ok) throw new JobOverlapException(key);
    try {
      return pjp.proceed();
    } finally {
      if (rl.isHeldByCurrentThread()) rl.unlock();
    }
  }
}
