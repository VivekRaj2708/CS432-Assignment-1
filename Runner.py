from Utils.Network import fetch_data
from Utils.MapRegister import MapRegister

if __name__ == "__main__":
    register = MapRegister()
    data = fetch_data() 
    prv_data = None
    for item in tuple(data.keys()):
        
        # print(item)
        
        inRegister = register.CheckInRegister(item)
        if inRegister:
            # print("[FOUND]" + inRegister)
            continue
        
        closest = register.GetClosest(item)
        if closest:
            # print("[FOUND]" + closest)
            register.AddToRegister(duplicate=item, value=closest)
        else:
            register.AddNewKey(item)
            # print("[ADDED]" + item)
    prv_data = data
        
    print(register.super)
        
