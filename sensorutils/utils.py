import datetime as dt
from dataclasses import dataclass
from typing import NamedTuple, Optional, Tuple
import pandas as pd


@dataclass
class TimeInterval:
    start: dt.datetime
    end: dt.datetime
    mult: Optional[float] = 20

    def __repr__(self) -> str:
        tm = (self.end - self.start)
        drw = f's:{self.start:%Y%m%d}|' + ''.join(['-' for i in range(tm.days//self.mult)]) + f'|e:{self.end:%Y%m%d}'
        return drw

    def __post_init__(self):
        if self.end < self.start:
            raise ValueError("Starting time must be smaller than ending time")
        

    def draw(self, y: 'TimeInterval') -> str:
        first = str(self)
        second = str(y)
        dst = (y.start - self.start).days
        dst_str =  ''.join([" " for i in range(dst//self.mult)])
        if dst > 0:
            space = f"""{first} {dst_str} \n {second}""".ljust(100)
        else:
            space = f"""{dst_str} {first} \n {second} """.ljust(100)
        return space

    def precedes(self, y: 'TimeInterval') -> bool:
        return (self.start < y.start) and (self.end < y.end)
    
    def meets(self, y: 'TimeInterval') -> bool:
        return self.end == y.start
    
    def overlaps(self, y: 'TimeInterval') -> bool:

        return (self.start < y.start)  and (y.end > self.end)
    
    def contains(self,  y: 'TimeInterval') -> bool:
        return (self.start < y.start)  and  (self.end > y.end)
    
    def starts(self, y: 'TimeInterval'):
        return (self.start == y.start)  and (y.end > self.end)
    
    def equals(self, y: 'TimeInterval'):
        return (self.start == y.start) and (self.end == y.end)
    
    def finished_by(self, y: 'TimeInterval') -> bool:
        return (self.start < y.start) and (self.end == y.end)


