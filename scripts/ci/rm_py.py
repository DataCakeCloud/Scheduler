# -*- coding: utf-8 -*-
import argparse
import os

def compile_py_to_so(directory):
    for root, dirs, files in os.walk(directory, followlinks=True):
        for file in files:
            if file.endswith(".py") and file != "__init__.py" and file != "state.py":
                py_file = os.path.join(root, file)
                so_file = py_file[:-2] + "so"
                c_file = py_file[:-2] + "c"
                pyc_file = py_file[:-2] + "pyc"
                if os.path.exists(pyc_file) and os.path.exists(py_file):
                    os.remove(py_file)

def main():
    parser = argparse.ArgumentParser(description="Compile .py files to .so files in the specified directory.")
    parser.add_argument("directory", help="The directory to compile .py files.")
    args = parser.parse_args()

    directory_to_compile = args.directory
    compile_py_to_so(directory_to_compile)

if __name__ == "__main__":
    main()
