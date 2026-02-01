from Utils.Algo import levenshtein_distance

class MapRegister:
    def __init__(self):
        self.map = {}
        self.super = {}
        
    def __normalize(self, key):
        if not isinstance(key, (str, int, float)):
            return str(key)
        # Handle Type Drifting: coerce to string and strip formatting
        s = str(key).lower()
        return "".join(filter(str.isalnum, s))

    def GetClosest(self, label):
        norm_label = self.__normalize(label)
        if not norm_label: return None
        
        keys = list(self.super.keys())
        best_match = None
        highest_similarity = 0.0

        for key in keys:
            norm_key = self.__normalize(key)
            
            # 1. Direct Normalized Match (Case/Snake/Camel issues)
            if norm_label == norm_key:
                return key

            # 2. Heuristic: Common Prefix/Substring (Handles IP vs IPAddress)
            # If one starts with the other and they are reasonably similar in length
            if norm_key.startswith(norm_label) or norm_label.startswith(norm_key):
                # Calculate "Overlap Ratio"
                ratio = min(len(norm_label), len(norm_key)) / max(len(norm_label), len(norm_key))
                if ratio > 0.4: # Adjustable threshold: 'ip' (2) vs 'ipaddress' (9) = 0.22 (might need lower)
                    return key

            # 3. Fuzzy fallback for typos
            dist = levenshtein_distance(norm_label, norm_key)
            # Normalize distance by string length so 'id' vs 'is' isn't the same as 'address' vs 'addresz'
            similarity = 1 - (dist / max(len(norm_label), len(norm_key)))
            
            if similarity > 0.8 and similarity > highest_similarity:
                highest_similarity = similarity
                best_match = key
        
        return best_match
    
    def AddToRegister(self, duplicate, value):
        self.map[duplicate] = value
        self.super[value].append(duplicate)
    
    def AddNewKey(self, key):
        self.map[key] = key
        self.super[key] = []

    def CheckInRegister(self, key):
        return self.map.get(key, None)