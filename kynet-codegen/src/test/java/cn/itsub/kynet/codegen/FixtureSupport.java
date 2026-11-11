package cn.itsub.kynet.codegen;

import cn.itsub.kynet.codegen.ir.IrModel;
import cn.itsub.kynet.codegen.scan.ClasspathScanner;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/** 测试共享：扫描 test-classes 上的 fixture 控制器得到 IrModel。 */
public final class FixtureSupport {
    private static IrModel model;

    private FixtureSupport() {
    }

    public static synchronized IrModel scanFixtures() {
        if (model != null) {
            return model;
        }
        try {
            Path testClasses = Path.of(
                    FixtureSupport.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            model = new ClasspathScanner().scan(List.of(testClasses),
                    List.of("cn.itsub.kynet.codegen.fixture"), Instant.parse("2026-01-01T00:00:00Z").toString());
            return model;
        } catch (Exception e) {
            throw new IllegalStateException("fixture 扫描失败", e);
        }
    }
}
