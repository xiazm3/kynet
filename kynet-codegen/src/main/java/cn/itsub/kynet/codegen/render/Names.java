package cn.itsub.kynet.codegen.render;

/** 生成器命名约定：PascalCase/camelCase/snake_case/kebab-case 互转。 */
public final class Names {
    private Names() {
    }

    public static String pascal(String name) {
        return capitalize(name);
    }

    public static String camel(String name) {
        return decapitalize(name);
    }

    public static String snake(String name) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                out.append('_');
            }
            out.append(Character.toLowerCase(c));
        }
        return out.toString();
    }

    public static String kebab(String name) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                out.append('-');
            }
            out.append(Character.toLowerCase(c));
        }
        return out.toString();
    }

    private static String capitalize(String name) {
        return name == null || name.isEmpty() ? name : Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    private static String decapitalize(String name) {
        return name == null || name.isEmpty() ? name : Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }
}
