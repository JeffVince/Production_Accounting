import ast
import os
import re
import sys

class LoggerTransformer(ast.NodeTransformer):
    """
    AST Transformer that prepends the function name (and optional variable data)
    to the log message string for self.logger.info/debug/warning/error calls.
    """
    item_id_pattern = re.compile('(Item\\s*ID\\s*=\\s*(\\S+))', re.IGNORECASE)

    def __init__(self, current_function_name: str=''):
        super().__init__()
        self.current_function_name = current_function_name

    def visit_Call(self, node: ast.Call) -> ast.AST:
        """
        - Check if node is a call to self.logger.<level> (e.g. info, debug, etc.)
        - If so, try to rewrite the first argument (the log message).
        """
        if not (isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Attribute) and (node.func.value.attr == 'logger') and (node.func.attr in {'info', 'debug', 'warning', 'error', 'exception', 'critical'})):
            return self.generic_visit(node)
        if not node.args:
            return self.generic_visit(node)
        first_arg = node.args[0]

        def build_new_message_str(original_text: str) -> str:
            """
            Insert function name bracket and parse out 'Item ID = foo' if present.
            Examples:
              Original: "We did xyz"
              => "[MyFunction] - We did xyz"

              Original: "Something happened. Item ID = 42"
              => "[MyFunction] [Item ID = 42] Something happened."
            """
            match = self.item_id_pattern.search(original_text)
            if match:
                item_id_segment = match.group(1)
                remainder = original_text.replace(item_id_segment, '').strip()
                new_msg = f'[{self.current_function_name}] [{item_id_segment}] {remainder}'
            else:
                new_msg = f'[{self.current_function_name}] - {original_text}'
            return new_msg
        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
            original_msg = first_arg.value
            new_msg = build_new_message_str(original_msg)
            node.args[0] = ast.Constant(value=new_msg)
            return self.generic_visit(node)
        if isinstance(first_arg, ast.JoinedStr):
            raw_str = ''
            for value_node in first_arg.values:
                if isinstance(value_node, ast.Constant) and isinstance(value_node.value, str):
                    raw_str += value_node.value
                else:
                    raw_str += '{expr}'
            new_prefix = ''
            match = self.item_id_pattern.search(raw_str)
            if match:
                item_id_segment = match.group(1)
                remainder = raw_str.replace(item_id_segment, '').strip()
                new_prefix = f'[{self.current_function_name}] [{item_id_segment}] '
                new_values = []
                leftover_text = raw_str
                leftover_text = leftover_text.replace(item_id_segment, '', 1)
                new_fstring_values = []
                for val in first_arg.values:
                    if isinstance(val, ast.Constant) and isinstance(val.value, str):
                        new_val_str = val.value.replace(item_id_segment, '')
                        if new_val_str:
                            new_fstring_values.append(ast.Constant(value=new_val_str))
                    else:
                        new_fstring_values.append(val)
                final_values = [ast.Constant(value=f'[{self.current_function_name}] [{item_id_segment}] ')] + new_fstring_values
                first_arg.values = final_values
            else:
                prefix_node = ast.Constant(value=f'[{self.current_function_name}] - ')
                first_arg.values.insert(0, prefix_node)
            return self.generic_visit(node)
        if isinstance(first_arg, ast.BinOp) and isinstance(first_arg.left, ast.Constant) and isinstance(first_arg.left.value, str) and isinstance(first_arg.op, ast.Mod):
            original_msg = first_arg.left.value
            new_msg = build_new_message_str(original_msg)
            new_left = ast.Constant(value=new_msg)
            new_binop = ast.BinOp(left=new_left, op=ast.Mod, right=first_arg.right)
            node.args[0] = new_binop
            return self.generic_visit(node)
        if isinstance(first_arg, ast.Call) and isinstance(first_arg.func, ast.Attribute) and isinstance(first_arg.func.value, ast.Constant) and isinstance(first_arg.func.value.value, str) and (first_arg.func.attr == 'format'):
            original_msg = first_arg.func.value.value
            new_msg = build_new_message_str(original_msg)
            new_call = ast.Call(func=ast.Attribute(value=ast.Constant(value=new_msg), attr='format', ctx=ast.Load()), args=first_arg.args, keywords=first_arg.keywords)
            node.args[0] = new_call
            return self.generic_visit(node)
        bracket_string = ast.Constant(value=f'[{self.current_function_name}] - ')
        plus_op = ast.BinOp(left=bracket_string, op=ast.Add(), right=first_arg)
        node.args[0] = plus_op
        return self.generic_visit(node)

class FunctionVisitor(ast.NodeVisitor):
    """
    Visits each function, applies the LoggerTransformer with the function name.
    """

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        transformer = LoggerTransformer(current_function_name=node.name)
        transformer.visit(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        transformer = LoggerTransformer(current_function_name=node.name)
        transformer.visit(node)
        self.generic_visit(node)

def process_file(filepath: str):
    """
    1) Parse the Python file into an AST,
    2) Visit each function and transform logger calls,
    3) Overwrite the file with the updated code.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        original_source = f.read()
    try:
        tree = ast.parse(original_source)
    except SyntaxError as e:
        print(f'Skipping {filepath} due to syntax error: {e}')
        return
    visitor = FunctionVisitor()
    visitor.visit(tree)
    try:
        new_source = ast.unparse(tree)
    except AttributeError:
        import astor
        new_source = astor.to_source(tree)
    if new_source == original_source:
        return
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_source)
    print(f'Updated logger calls in: {filepath}')

def is_python_file(filename: str) -> bool:
    return filename.endswith('.py') and (not filename.startswith('.'))

def main(root_path: str):
    for (dirpath, dirnames, filenames) in os.walk(root_path):
        for filename in filenames:
            if is_python_file(filename):
                full_path = os.path.join(dirpath, filename)
                process_file(full_path)
if __name__ == '__main__':
    if len(sys.argv) > 1:
        root_dir = sys.argv[1]
    else:
        root_dir = '../'
    main(root_dir)