


class Stra:

    def __init__(self):
        self.short = [2, 3, 4, 5]
        self.long = [1, 2, 3, 4]
    
    def on_return_pos(self):
        res = []
        for i in range(0, 4):
            if self.short[i] - self.long[i] > 0:
                res.append(1)
            elif self.short[i] - self.long[i] < 0:
                res.append(-1)
            else:
                res.append(0)
        return res

        
    def on_return_pos_closure(self, i):
        long_mv_average = ctx.ma(30)
        short_mv_average = ctx.ma(30)
        if self.short[i] - self.long[i] > 0:
            return 1.
        elif self.short[i] - self.long[i] < 0:
            return -1.
        else:
            return 0.

    @RustClosureParser
    def on_return_pos_clousre(self):
        lambda i: 
            if self.short[i] - self.long[i] > 0:
                return 1.
            elif self.short[i] - self.long[i] < 0:
                return -1.
            else:
                return 0.     
        