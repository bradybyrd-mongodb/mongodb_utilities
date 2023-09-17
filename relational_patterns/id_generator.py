import random

#  BJB 5/19/22
#  Simple class to track ids for multiple modules
class Id_generator:
    def __init__(self, details = {}):
        prefix = "none"
        tally = 100000
        size = 100
        if "seed" in details:
            tally = details["seed"]
        if "size" in details:
            size = details["size"]
        self.value_history = {"none" : {"base" : tally, "prev" : tally, "cur" : tally, "size" : size}}
        if "prefix" in details:
            prefix = details["prefix"]
            self.value_history[prefix] = {"base" : tally, "prev" : tally, "cur" : tally, "size" : size}

    def set(self, details):
        cur = self.value_history["none"]
        if "prefix" in details:
            prefix = details["prefix"]
        if "prefix" in self.value_history:
            cur = self.value_history[prefix]
        seed = details["seed"]
        size = details["size"]
        self.value_history[prefix] = {"base" : seed, "prev" : cur["prev"], "cur" : seed, "size" : size}
        return(seed)

    def random_value(self, prefix, base = 1000, top = 1000000):
        if prefix in self.value_history:
            base = self.value_history[prefix]["base"]
            top = self.value_history[prefix]["base"] + self.value_history[prefix]["size"]
        else:
            top = base + top
        return(f'{prefix}{random.randint(base,top)}')

    def get(self, prefix = "none", amount = 1):
        #bb.logit(f'IDGEN - ValueHist: {self.value_history}')
        if prefix in self.value_history:
            cur = self.value_history[prefix]
        else:
            it = self.value_history["none"]
            cur = {"base" : it["base"], "prev" : it["base"], "cur" : it["base"], "size" : it["size"]}
        new = {"base" : cur["base"], "prev" : cur["cur"], "cur" : cur["cur"] + amount, "size" : cur["size"]}
        self.value_history[prefix] = new
        return f'{prefix}{new["prev"]}'

