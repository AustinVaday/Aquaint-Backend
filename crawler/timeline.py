import multihead

class Event:
    def __init__(self, user, event, other, time):
        self.user  = user
        self.event = event
        self.other = other
        self.time  = time
    
    def classes(self):
        return ('%s, %s, %s, %s' %
            (
                type(self.user),
                type(self.event),
                type(self.other),
                type(self.time)
            )
        )
    
    @classmethod
    def from_dynamo(cls, user, dynamo_dict):
        return cls(
            user,
            dynamo_dict['event'],
            dynamo_dict['otheruser'] if 'otheruser' in dynamo_dict
                else list(dynamo_dict['otherusers'][0]), # This is a major problem with the data schema
            int(dynamo_dict['time'])
        )
    
class Aggregator:
    def __init__(self):
        self.sorter = multihead.Sorter(lambda event: event.time)
    
    def load(self, user, events):
        self.sorter.load(events)
    
    def sort(self, count):
        # (1..count).map { @sorter.pop }.compress.reverse
        return list(
            reversed(
                filter(lambda x: x is not None,
                    map(
                        lambda _: self.sorter.pop(),
                        range(count)
                    )
                )
            )
        )
