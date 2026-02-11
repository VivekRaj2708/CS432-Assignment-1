from Utils.Log import logger

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
        assert type_ in ["int", "string", "float", "bool", "list", "UNK"], "Unsupported type"
        assert not auto or type_ == "int", "AUTO can only be set for int type"
        if type_ == "UNK":
            logger.warning("Type UNK is not recommended for Metadata")
        
        self.type = type_
        self.subtype = None
        self.auto = auto
        self.current_value = 0 if auto else None
        
    
    def __repr__(self):
        return f"Metadata(type={self.type}" + (", AUTO" if self.auto else "") + ")"

    def resolveValue(self, value=None):
        if self.auto:
            self.current_value += 1
            return self.current_value
        if self.type == "UNK":
            logger.debug("Resolving value for UNK type")
            if value is None:
                logger.warning("Value is None for UNK type; returning None")
                return None
            if isinstance(value, (int, float, str, bool, list)):
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
                
                if isinstance(value, str):
                    self.type = "string"
                    self.auto = False
                    logger.info(f"Resolved UNK type to string based on provided value")
                    return value
                
                logger.error(f"Unable to resolve UNK type for value: {value} of type {type(value)}; returning None")
                raise ValueError(f"Unable to resolve UNK type for value: {value} of type {type(value)}")
        
        elif self.type == "int":
            try:
                return int(value)
            except ValueError:
                logger.error(f"Cannot convert value {value} to int")
                raise ValueError(f"Cannot convert value {value} to int")
        
        elif self.type == "string":
            try:
                return str(value)
            except ValueError:
                logger.error(f"Cannot convert value {value} to string")
                raise ValueError(f"Cannot convert value {value} to string")
        
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
                list_value = eval(value)
                if isinstance(list_value, list):
                    return_list_data = []
                    if len(list_value) > 0:
                        for x in value:
                            return_list_data.append(self.subtype.resolveValue(x))
                    return return_list_data
                else:
                    raise ValueError(f"Value {value} is not a list")
            except (SyntaxError, NameError, ValueError):
                logger.error(f"Cannot convert value {value} to list")
                raise ValueError(f"Cannot convert value {value} to list")
        
        else:
            logger.error(f"Unsupported Metadata type {self.type}")
            raise ValueError(f"Unsupported Metadata type {self.type}, {value}")
        
        return value
        