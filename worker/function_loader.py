#!/usr/bin/env python3
"""
Dynamic Function Loader for MapReduce User Functions
Loads user-provided Python modules containing map, reduce, and combiner functions
"""

import importlib.util
import sys
import os


class FunctionLoader:
    """Dynamically loads user-provided map/reduce functions from Python files"""

    def __init__(self, map_reduce_file: str):
        """
        Initialize the function loader

        Args:
            map_reduce_file: Path to user's Python file containing map/reduce functions
        """
        self.map_reduce_file = map_reduce_file
        self.module = None

    def load_module(self):
        """
        Dynamically load user-provided module

        Returns:
            The loaded module object

        Raises:
            FileNotFoundError: If the map/reduce file doesn't exist
        """
        if not os.path.exists(self.map_reduce_file):
            raise FileNotFoundError(f"Map/Reduce file not found: {self.map_reduce_file}")

        spec = importlib.util.spec_from_file_location("user_mapreduce", self.map_reduce_file)
        module = importlib.util.module_from_spec(spec)
        sys.modules["user_mapreduce"] = module
        spec.loader.exec_module(module)
        self.module = module
        return module

    def get_map_function(self):
        """
        Get map function from loaded module

        Returns:
            The map_function callable from the module

        Raises:
            AttributeError: If module doesn't define 'map_function'
        """
        if not self.module:
            self.load_module()

        if not hasattr(self.module, 'map_function'):
            raise AttributeError("Module must define 'map_function'")
        return self.module.map_function

    def get_reduce_function(self):
        """
        Get reduce function from loaded module

        Returns:
            The reduce_function callable from the module

        Raises:
            AttributeError: If module doesn't define 'reduce_function'
        """
        if not self.module:
            self.load_module()

        if not hasattr(self.module, 'reduce_function'):
            raise AttributeError("Module must define 'reduce_function'")
        return self.module.reduce_function

    def get_combiner_function(self):
        """
        Get combiner function from loaded module

        Returns:
            The combiner_function callable, or reduce_function as default, or None
        """
        if not self.module:
            self.load_module()

        # First try to get explicit combiner_function
        if hasattr(self.module, 'combiner_function'):
            return self.module.combiner_function
        # Default combiner is reduce function
        elif hasattr(self.module, 'reduce_function'):
            return self.module.reduce_function
        # No combiner available
        return None
