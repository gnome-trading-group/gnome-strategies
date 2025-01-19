package group.gnometrading.books;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class BSTUtilsTest {

    private Map<Integer, Limit> limitMap;

    @BeforeEach
    void setup() {
        this.limitMap = new HashMap<>();
    }

    private static Stream<Arguments> testInsertArguments() {
        return Stream.of(
                Arguments.of("", 1, "1"),
                Arguments.of("1[N,2]", 4, "1[N,2[N,4]]"),
                Arguments.of("10[5[3,6],15[11,17[N,20]]]", 21, "10[5[3,6],15[11,17[N,20[N,21]]]]"),
                Arguments.of("10[5[3,6],15[11,17[N,20]]]", 16, "10[5[3,6],15[11,17[16,20]]]"),
                Arguments.of("10[5[3,6],15[11,17[N,20]]]", 8, "10[5[3,6[N,8]],15[11,17[N,20]]]")
        );
    }

    @ParameterizedTest
    @MethodSource("testInsertArguments")
    void testInsert(String initial, int insert, String result) {
        Limit t1 = createTree(initial, new int[] {0});
        Limit res = BSTUtils.insert(t1, createLimit(insert));
        assertTrees(createTree(result, new int[] {0}), res, null, null);
    }

    private static Stream<Arguments> testRemoveArguments() {
        return Stream.of(
                Arguments.of("1", 1, ""),
                Arguments.of("1[N,2]", 2, "1"),
                Arguments.of("1[N,2]", 1, "2"),
                Arguments.of("1[0,N]", 1, "0"),
                Arguments.of("1[N,2[0,4]]", 1, "2[0,4]"),
                Arguments.of("3[2,5[N,7]]", 3, "5[2,7]"),
                Arguments.of("3[2,7[4[N,6],N]]", 3, "4[2,7[6,N]]")
        );
    }

    @ParameterizedTest
    @MethodSource("testRemoveArguments")
    void testRemove(String initial, int remove, String result) {
        Limit t1 = createTree(initial, new int[] {0});
        Limit res = BSTUtils.remove(t1, this.limitMap.get(remove));
        assertTrees(createTree(result, new int[] {0}), res, null, null);
    }

    private Limit createTree(String tree, int[] index) {
        if (index[0] >= tree.length()) {
            return null;
        }

        int num = 0;
        while (index[0] < tree.length()) {
            if (tree.charAt(index[0]) == 'N') {
                index[0]++;
                return null;
            }

            if (tree.charAt(index[0]) == '[') {
                index[0]++; // skip opening bracket
                Limit l1 = createLimit(num);
                l1.left = createTree(tree, index);
                if (l1.left != null) l1.left.parent = l1;
                index[0]++; // skip the comma
                l1.right = createTree(tree, index);
                if (l1.right != null) l1.right.parent = l1;
                index[0]++; // skip closing bracket
                return l1;
            }

            if (tree.charAt(index[0]) == ']') {
                return createLimit(num);
            }

            if (tree.charAt(index[0]) == ',') {
                return createLimit(num);
            }

            num = (num * 10) + (tree.charAt(index[0]) - '0');
            index[0]++;
        }

        return createLimit(num);
    }

    private Limit createLimit(int price) {
        Limit l1 = new Limit();
        l1.limitPrice = price;
        limitMap.put(price, l1);
        return l1;
    }


    private static void assertTrees(Limit tree1, Limit tree2, Limit tree1Parent, Limit tree2Parent) {
        if (tree1 == null || tree2 == null) {
            assertEquals(tree1, tree2);
        } else {
            assertEquals(tree1.limitPrice, tree2.limitPrice);
            assertEquals(tree1.parent, tree1Parent);
            assertEquals(tree2.parent, tree2Parent);

            assertTrees(tree1.left, tree2.left, tree1, tree2);
            assertTrees(tree1.right, tree2.right, tree1, tree2);
        }
    }
}