from Utils.Algo import levenshtein_distance

class MapRegister:
    def __init__(self):
        self.map = {}
        self.super = {}

    def GetClosest(self, label):
        keys = list(self.super.keys())
        closest = None
        closest_dist = 2  # We only consider distance <= 2
        for key in keys:
            dist = levenshtein_distance(label, key)
            if dist <= closest_dist:
                closest_dist = dist
                closest = key
        return closest
    
    def AddToRegister(self, duplicate, value):
        self.map[duplicate] = value
        self.super[value].append(duplicate)
    
    def AddNewKey(self, key):
        self.map[key] = key
        self.super[key] = []

    def CheckInRegister(self, key):
        return self.map.get(key, None)