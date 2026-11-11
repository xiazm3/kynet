package cn.itsub.kynet.codegen.scan;

import cn.itsub.kynet.codegen.FixtureSupport;
import cn.itsub.kynet.codegen.ir.ActionMeta;
import cn.itsub.kynet.codegen.ir.FieldMeta;
import cn.itsub.kynet.codegen.ir.MessageMeta;
import cn.itsub.kynet.codegen.ir.TypeKind;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClasspathScannerTest {

    private static Optional<ActionMeta> action(int cmd, int act) {
        return FixtureSupport.scanFixtures().actions().stream()
                .filter(a -> a.cmd() == cmd && a.act() == act).findFirst();
    }

    private static Optional<FieldMeta> field(String message, String name) {
        return FixtureSupport.scanFixtures().messages().stream()
                .filter(m -> m.name().equals(message)).findFirst()
                .flatMap(m -> m.fields().stream().filter(f -> f.name().equals(name)).findFirst());
    }

    @Test
    void messageActionsAreResolved() {
        ActionMeta loginVerify = action(9, 1).orElseThrow();
        assertEquals(TypeKind.MESSAGE, loginVerify.paramType().kind());
        assertEquals("cn.itsub.kynet.codegen.fixture.FixtureMessages$FixtureLoginVerifyMessage",
                loginVerify.paramType().message());
        assertEquals(TypeKind.MESSAGE, loginVerify.returnType().kind());
        assertEquals("fixtureLoginVerifyMessage", loginVerify.paramName());
        assertFalse(loginVerify.needsContext());
    }

    @Test
    void primitiveAndListReturnsAreResolved() {
        assertEquals(TypeKind.INT32, action(9, 2).orElseThrow().returnType().kind());
        ActionMeta listString = action(9, 3).orElseThrow();
        assertEquals(TypeKind.STRING, listString.returnType().kind());
        assertTrue(listString.returnType().repeated());
        assertEquals(TypeKind.STRING, listString.paramType().kind());
        ActionMeta listUsers = action(9, 4).orElseThrow();
        assertEquals(TypeKind.MESSAGE, listUsers.returnType().kind());
        assertTrue(listUsers.returnType().repeated());
    }

    @Test
    void voidAndContextOnlyActions() {
        ActionMeta fire = action(9, 5).orElseThrow();
        assertNull(fire.returnType());
        assertEquals(TypeKind.MESSAGE, fire.paramType().kind());
        ActionMeta contextOnly = action(9, 6).orElseThrow();
        assertNull(contextOnly.paramType());
        assertTrue(contextOnly.needsContext());
    }

    @Test
    void explicitOrdersAreKept() {
        MessageMeta user = FixtureSupport.scanFixtures().messages().stream()
                .filter(m -> m.name().equals("FixtureUserMessage")).findFirst().orElseThrow();
        assertEquals(6, user.fields().size());
        assertEquals(1, field("FixtureUserMessage", "userId").orElseThrow().order());
        assertEquals(5, field("FixtureUserMessage", "tags").orElseThrow().order());
        assertTrue(field("FixtureUserMessage", "tags").orElseThrow().repeated());
        assertEquals(TypeKind.ENUM, field("FixtureUserMessage", "level").orElseThrow().kind());
        assertEquals(TypeKind.MESSAGE, field("FixtureUserMessage", "inner").orElseThrow().kind());
    }

    @Test
    void implicitOrdersFollowJprotobufRule() {
        // max(显式 order)=4，隐式字段按声明顺序从 5 递增：implicitA=5、implicitB=6
        assertEquals(5, field("FixtureMixed", "implicitA").orElseThrow().order());
        assertEquals(3, field("FixtureMixed", "explicitC").orElseThrow().order());
        assertEquals(6, field("FixtureMixed", "implicitB").orElseThrow().order());
        assertEquals(2, field("FixtureMixed", "score").orElseThrow().order());
        assertEquals(4, field("FixtureMixed", "tags").orElseThrow().order());
    }

    @Test
    void enumsAreCollected() {
        var level = FixtureSupport.scanFixtures().enums().stream()
                .filter(e -> e.name().equals("FixtureLevel")).findFirst().orElseThrow();
        assertEquals(2, level.constants().size());
        assertEquals(0, level.constants().get(0).ordinal());
        assertEquals("LOW", level.constants().get(0).name());
    }
}
