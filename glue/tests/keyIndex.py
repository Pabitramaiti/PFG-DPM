class DataStorage:
    def __init__(self, json_data):
        self.json_data = json_data

    def get_data(self):
        return self.json_data

    def add_record(self, record):
        self.json_data.append(record)

    def remove_record(self, record):
        if record in self.json_data:
            self.json_data.remove(record)
        else:
            print("Record not found.")

    def get_record_by_index(self, index):
        if 0 <= index < len(self.json_data):
            return self.json_data[index]
        else:
            print("Invalid index.")
            return None

    def update_record(self, old_record, new_record):
        if old_record in self.json_data:
            index = self.json_data.index(old_record)
            self.json_data[index] = new_record
        else:
            print("Record not found.")


class KINode:
    def __init__(self, key, index):
        self.key = key
        self.left = None
        self.right = None
        self.height = 1
        self.index = index


class KITree:
    def __init__(self):
        self.root = None

    def insert(self, key, index):
        self.root = self._insert(self.root, key, index)

    def _insert(self, node, key, index):
        if not node:
            return KINode(key, index)
        elif key < node.key:
            node.left = self._insert(node.left, key, index)
        else:
            node.right = self._insert(node.right, key, index)

        node.height = 1 + max(self._get_height(node.left), self._get_height(node.right))

        balance = self._get_balance(node)

        if balance > 1 and key < node.left.key:
            return self._rotate_right(node)
        if balance < -1 and key > node.right.key:
            return self._rotate_left(node)
        if balance > 1 and key > node.left.key:
            node.left = self._rotate_left(node.left)
            return self._rotate_right(node)
        if balance < -1 and key < node.right.key:
            node.right = self._rotate_right(node.right)
            return self._rotate_left(node)

        return node

    def get(self, key):
        return self._get(self.root, key)

    def isExist(self, key):
        return self._isExist(self.root, key)

    def _isExist(self, node, key):
        if not node:
            return False
        if node.key == key:
            return True
        elif key < node.key:
            return self._isExist(node.left, key)
        else:
            return self._isExist(node.right, key)

    def _get(self, node, key):
        if not node:
            return None
        if node.key == key:
            return node.index
        elif key < node.key:
            return self._get(node.left, key)
        else:
            return self._get(node.right, key)

    def _get_height(self, node):
        if not node:
            return 0
        return node.height

    def _get_balance(self, node):
        if not node:
            return 0
        return self._get_height(node.left) - self._get_height(node.right)

    def _rotate_left(self, z):
        y = z.right
        T2 = y.left

        y.left = z
        z.right = T2

        z.height = 1 + max(self._get_height(z.left), self._get_height(z.right))
        y.height = 1 + max(self._get_height(y.left), self._get_height(y.right))

        return y

    def _rotate_right(self, z):
        y = z.left
        T3 = y.right

        y.right = z
        z.left = T3

        z.height = 1 + max(self._get_height(z.left), self._get_height(z.right))
        y.height = 1 + max(self._get_height(y.left), self._get_height(y.right))

        return y
