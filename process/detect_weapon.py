

class WeaponDetector:
    def __init__(self,text):
        self.text = text
        try:
            with open("data/weapon_list.txt","r") as f:
                self.weapons = f.read().split("\n")
        except Exception as e:
            raise Exception(f"file didn't upload good. error: {e}")
    def detect_weapons(self):
        all_weapons = []
        for weapon in self.weapons:
            if weapon.lower() in self.text:
                all_weapons.append(weapon.lower())
        return all_weapons or ""
