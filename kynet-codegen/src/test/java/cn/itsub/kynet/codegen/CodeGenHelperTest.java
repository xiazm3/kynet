package cn.itsub.kynet.codegen;

import cn.itsub.kynet.codegen.ir.IrModel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CodeGenHelperTest {
    @TempDir
    Path temp;

    private static Path fixtureClasses() throws Exception {
        return Path.of(FixtureSupport.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    @Test
    void defaultScanFindsCurrentProjectActionsAndGeneratesCsharp() throws IOException {
        Path output = temp.resolve("default");
        var files = CodeGenHelper.configure().outputDirectory(output).generate();

        assertEquals(3, files.size());
        assertTrue(files.contains(output.resolve("FixtureMyActionProxy.g.cs")));
        assertTrue(Files.readString(output.resolve("FixtureMyActionProxy.g.cs"))
                .contains("LoginVerifyAsync("));
        assertFalse(Files.exists(output.resolve("fixture-my-action.ts")));
    }

    @Test
    void configurationOverridesTargetsAndUsesProjectRelativeOutput() throws Exception {
        Path root = temp.resolve("project");
        var files = CodeGenHelper.configure()
                .projectRoot(root)
                .classpathEntries(fixtureClasses())
                .includePackages("cn.itsub.kynet.codegen.fixture")
                .targets(ClientTarget.CSHARP, ClientTarget.GDSCRIPT)
                .outputDirectory(Path.of("client/generated"))
                .csharpNamespace("Demo.Client.Generated")
                .gdGeneratedPath("res://demo/generated")
                .generate();

        Path output = root.resolve("client/generated");
        assertEquals(5, files.size());
        assertTrue(Files.readString(output.resolve("FixtureMyActionProxy.g.cs"))
                .contains("namespace Demo.Client.Generated"));
        assertTrue(Files.readString(output.resolve("fixture_my_action.gd"))
                .contains("res://demo/generated/kynet_messages.gd"));
        assertFalse(Files.readString(output.resolve("fixture_my_action.gd"))
                .contains("_with_callback"));
    }

    @Test
    void defaultOutputIsUnderProjectAndIrCanBeReused() throws Exception {
        Path root = temp.resolve("project");
        var configured = CodeGenHelper.configure().projectRoot(root).classpathEntries(fixtureClasses());
        IrModel model = configured.scan();
        assertEquals(8, model.actions().size());
        Path ir = configured.scanTo(Path.of("contract/actions.json"));
        assertEquals(root.resolve("contract/actions.json"), ir);
        assertTrue(Files.exists(ir));

        var files = CodeGenHelper.configure().projectRoot(root).fromIr(ir)
                .target(ClientTarget.TYPESCRIPT).generate();
        assertEquals(2, files.size());
        assertTrue(files.contains(root.resolve("generated/kynet/fixture-my-action.ts")));
        assertTrue(Files.readString(root.resolve("generated/kynet/fixture-my-action.ts"))
                .contains("async loginVerify("));
    }

    @Test
    void cliStillAcceptsExplicitClasspathAndLanguage() throws Exception {
        Path output = temp.resolve("cli");
        CodeGenCli.run(new String[]{"all", "--classpath", fixtureClasses().toString(),
                "--include", "cn.itsub.kynet.codegen.fixture", "--lang", "csharp",
                "--out", output.toString()});
        assertTrue(Files.exists(output.resolve("FixtureMyActionProxy.g.cs")));
        assertFalse(Files.exists(output.resolve("fixture-my-action.ts")));

        Path ir = temp.resolve("cli/actions.json");
        CodeGenCli.run(new String[]{"scan", "--classpath", fixtureClasses().toString(),
                "--include", "cn.itsub.kynet.codegen.fixture", "--out", ir.toString()});
        assertTrue(Files.exists(ir));
        Path fromIr = temp.resolve("cli/from-ir");
        CodeGenCli.run(new String[]{"gen", "--ir", ir.toString(), "--lang", "typescript",
                "--out", fromIr.toString()});
        assertTrue(Files.exists(fromIr.resolve("fixture-my-action.ts")));
    }

    @Test
    void projectRootChangesOutputWithoutHidingRuntimeActions() throws IOException {
        Path root = temp.resolve("project");
        var files = CodeGenHelper.configure().projectRoot(root).generate();

        assertTrue(files.contains(root.resolve("generated/kynet/FixtureMyActionProxy.g.cs")));
    }

    @Test
    void languageSpecificOptionsReachTheirRenderers() throws Exception {
        Path output = temp.resolve("options");
        var files = CodeGenHelper.configure()
                .classpathEntries(fixtureClasses())
                .targets(ClientTarget.TYPESCRIPT, ClientTarget.GDSCRIPT, ClientTarget.PROTO)
                .outputDirectory(output)
                .tsImportPrefix("../../sdk")
                .gdAddonPath("res://myaddon")
                .gdGeneratedPath("res://myaddon/generated")
                .protoPackage("demo.contract")
                .generate();

        assertEquals(5, files.size());
        assertTrue(Files.readString(output.resolve("fixture-my-action.ts"))
                .contains("from \"../../sdk/client.js\""));
        assertTrue(Files.readString(output.resolve("kynet_messages.gd"))
                .contains("preload(\"res://myaddon/protobuf.gd\")"));
        assertTrue(Files.readString(output.resolve("fixture_my_action.gd"))
                .contains("res://myaddon/generated/kynet_messages.gd"));
        assertTrue(Files.readString(output.resolve("kynet-actions.proto"))
                .contains("package demo.contract;"));
    }

    @Test
    void invalidOrEmptySelectionFailsClearly() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> CodeGenHelper.configure().targets());
        assertThrows(IllegalArgumentException.class, () -> CodeGenHelper.configure()
                .classpathEntries(fixtureClasses()).includePackages("missing.actions").scan());
    }
}
