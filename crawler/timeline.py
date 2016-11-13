import multihead

# Internal representation of timeline/newsfeed events
class Event:
    # Create new event
    def __init__(self, user, event, other, time):
        self.user  = user
        self.event = event
        self.other = other
        self.time  = time
    
    # Convert DynamoDB list entry to new event
    @classmethod
    def from_dynamo(cls, user, dynamo_dict):
        return cls(
            user,
            dynamo_dict['event'],
            dynamo_dict['other'],
            int(dynamo_dict['time'])
        )

    # Show class names of member variables (for debug)
    def classes(self):
        return ('%s, %s, %s, %s' %
            (
                type(self.user),
                type(self.event),
                type(self.other),
                type(self.time)
            )
        )

# Lazy sorter wrapper class
class Aggregator:
    # Instantiate sorter to sort by event time
    def __init__(self):
        self.sorter = multihead.Sorter(lambda event: event.time)
    
    # Load event list into sorter
    def load(self, events):
        self.sorter.load(events)
    
    # Lazy sort up to N items, shrinking list to fit
    def sort(self, count):
        return filter(
            lambda x: x is not None,
            map(
                lambda _: self.sorter.pop(),
                range(count)
            )
        )
