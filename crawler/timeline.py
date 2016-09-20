import multihead

class Event:
    def __init__(self, user, event, other, time):
        self.user  = user
        self.event = event
        self.other = other
        self.time  = time
    
    @classmethod
    def from_dynamo(cls, user, dynamo_dict):
        return cls(
            user,
            dynamo_dict['event'],
            dynamo_dict['otheruser'],
            dynamo_dict['time']
        )
    
class Aggregator:
    def __init__(self):
        self.sorter = multihead.Sorter(lambda event: event.time)
    
    def load(self, user, events):
        self.sorter.load(events)
    
    def sort(self, count):
        return filter(lambda x: x is not None,
            map(
                lambda _: self.sorter.pop(),
                range(count)
            )
        )
