from Utils.Log import logger
from json import loads

def CheckBool(value):
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        if value.lower() in ['true', '1', 'yes']:
            return True
        elif value.lower() in ['false', '0', 'no']:
            return True
    if isinstance(value, int):
        if value == 0 or value == 1:
            return True
    return False

def ResolveBool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in ['true', '1', 'yes']:
            return True
        elif value.lower() in ['false', '0', 'no']:
            return False
    if isinstance(value, int):
        if value == 0:
            return False
        elif value == 1:
            return True
    raise ValueError(f"Cannot convert {value} to bool")

class Metadata:
    
    def __init__(self, type_="UNK", auto = False):
        assert type_ in ["int", "str", "float", "bool", "list", "UNK"], "Unsupported type"
        assert not auto or type_ == "int", "AUTO can only be set for int type"
        if type_ == "UNK":
            logger.warning("Type UNK is not recommended for Metadata")
        
        self.type = type_
        self.subtype = None
        self.auto = auto
        self.current_value = 0 if auto else None
        
    
    def __repr__(self):
        if self.type == "list":
            sub = self.subtype if self.subtype else "UNK"
            return f"Metadata(array<{sub}>)"
        return f"Metadata(type={self.type}{', AUTO' if self.auto else ''})"

    def resolveValue(self, value=None):
        if self.auto:
            self.current_value += 1
            return self.current_value
        if value is None and self.type != "UNK":
            logger.warning(f"No value provided for non-UNK type {self.type}; returning None")
            return None
        if self.type == "UNK":
            logger.debug("Resolving value for UNK type")
            if value is None:
                logger.warning("Value is None for UNK type; returning None")
                return None
            if isinstance(value, (int, float, bool, list)):
                self.type = type(value).__name__
                if self.type == "list":
                    return_list_data = []
                    if len(value) > 0:
                        self.subtype = Metadata(type_="UNK")
                        for x in value:
                            return_list_data.append(self.subtype.resolveValue(x))
                    else:
                        logger.warning("Empty list provided for UNK type; unable to determine subtype")
                    return return_list_data
                self.auto = False
                logger.info(f"Resolved UNK type to {self.type} based on provided value")
            else:
                logger.warning(f"Unsupported value type {type(value)} for UNK Metadata; Trying all combinations of basic types")
                try:
                    val = int(value)
                    self.type = "int"
                    self.auto = False
                    logger.info(f"Resolved UNK type to int based on provided value")
                    return val
                except ValueError:
                    logger.debug("Failed to resolve UNK type as int")
                try:
                    val = float(value)
                    self.type = "float"
                    self.auto = False
                    logger.info(f"Resolved UNK type to float based on provided value")
                    return val
                except ValueError:
                    logger.debug("Failed to resolve UNK type as float")

                if CheckBool(value):
                    value = ResolveBool(value)
                    self.type = "bool"
                    self.auto = False
                    logger.info(f"Resolved UNK type to bool based on provided value")
                    return value
            
                try:
                    val = loads(value)
                    if isinstance(val, list):
                        self.type = "list"
                        self.subtype = Metadata(type_="UNK")
                        return_list_data = []
                        if len(val) > 0:
                            for x in val:
                                return_list_data.append(self.subtype.resolveValue(x))
                        else:
                            logger.warning("Empty list provided for UNK type; unable to determine subtype")
                        logger.info(f"Resolved UNK type to list based on provided value")
                        return return_list_data
                except (ValueError, SyntaxError):
                    logger.debug("Failed to resolve UNK type as list")
                
                if isinstance(value, str):
                    self.type = "str"
                    self.auto = False
                    logger.info(f"Resolved UNK type to str based on provided value")
                    return value
                
                logger.error(f"Unable to resolve UNK type for value: {value} of type {type(value)}; returning None")
                raise ValueError(f"Unable to resolve UNK type for value: {value} of type {type(value)}")
        
        elif self.type == "int":
            try:
                return int(value)
            except ValueError:
                logger.error(f"Cannot convert value {value} to int")
                raise ValueError(f"Cannot convert value {value} to int")
        
        elif self.type == "str":
            try:
                return str(value)
            except ValueError:
                logger.error(f"Cannot convert value {value} to str")
                raise ValueError(f"Cannot convert value {value} to str")
        
        elif self.type == "float":
            try:
                return float(value)
            except ValueError:
                logger.error(f"Cannot convert value {value} to float")
                raise ValueError(f"Cannot convert value {value} to float")
        
        elif self.type == "bool":
            try:
                return ResolveBool(value)
            except ValueError:
                logger.error(f"Cannot convert value {value} to bool")
                raise ValueError(f"Cannot convert value {value} to bool")
        
        elif self.type == "list":
            try:
                # 1. Normalize input to a list
                if isinstance(value, str):
                    try:
                        value = loads(value)
                    except Exception:
                        # Fallback for non-strict JSON
                        import ast
                        value = ast.literal_eval(value)
                
                if not isinstance(value, list):
                    raise ValueError(f"Expected list, got {type(value)}")

                # 2. Ensure subtype exists (even if we didn't have one before)
                if self.subtype is None:
                    self.subtype = Metadata(type_="UNK")

                # 3. Process elements
                return_list_data = []
                for x in value:
                    return_list_data.append(self.subtype.resolveValue(x))
                
                return return_list_data # Ensure this is returned!

            except Exception as e:
                logger.error(f"List resolution failed: {e}")
                raise ValueError(f"Cannot convert value to list: {value}")
        
        else:
            logger.error(f"Unsupported Metadata type {self.type}")
            raise ValueError(f"Unsupported Metadata type {self.type}, {value}")
        
        return value
