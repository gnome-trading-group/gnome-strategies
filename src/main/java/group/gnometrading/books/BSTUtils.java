package group.gnometrading.books;

/**
 * Uses red-black tree balancing over AVL trees due to updates being a more dominant flow than reads.
 * AVL trees perform better when reading from the tree.
 */
public class BSTUtils {

    public static Limit insert(final Limit root, final Limit node) {
        Limit prev = null;
        Limit curr = root;

        while (curr != null) {
            prev = curr;
            if (node.limitPrice < curr.limitPrice) {
                curr = curr.left;
            } else {
                curr = curr.right;
            }
        }

        node.parent = prev;
        if (prev == null) {
            return node;
        }

        if (node.limitPrice < prev.limitPrice) {
            prev.left = node;
        } else {
            prev.right = node;
        }

        return root;
    }

    public static Limit remove(final Limit root, final Limit node) {
        if (node.left == null) {
            return replaceLimit(node, node.right, root);
        } else if (node.right == null) {
            return replaceLimit(node, node.left, root);
        } else {
            final Limit successor = findSuccessor(node);
            if (successor.parent != node) {
                replaceLimit(successor, successor.right, root);
                successor.right = node.right;
                successor.right.parent = successor;
            }

            final Limit replaced = replaceLimit(node, successor, root);
            successor.left = node.left;
            successor.left.parent = successor;
            return replaced;
        }
    }

    private static Limit replaceLimit(final Limit limit, final Limit replacement, final Limit root) {
        if (limit.parent != null) {
            if (limit.parent.left == limit) {
                limit.parent.left = replacement;
            } else {
                limit.parent.right = replacement;
            }
        }

        if (replacement != null) {
            replacement.parent = limit.parent;
        }

        return limit == root ? replacement : root;
    }

    private static Limit findSuccessor(final Limit limit) {
        if (limit.right != null) {
            return minimum(limit.right);
        }

        Limit current = limit;
        Limit parent = current.parent;
        while (parent != null) { // Go up the tree until we're a left child somewhere
            if (current != parent.right) {
                break;
            }
            current = parent;
            parent = parent.parent;
        }
        return parent;
    }

    private static Limit minimum(Limit limit) {
        while (limit.left != null) {
            limit = limit.left;
        }
        return limit;
    }

}
