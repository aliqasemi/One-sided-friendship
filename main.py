 
from abc import ABC
from mrjob.job import MRJob
from mrjob.step import MRStep

status = input("please inter couple or single(default = single) ... ")


def mapper_get_friends(_, line):
    user = line.split("\t")[0]
    if len(line.split("\t")) > 1:
        if "," in line.split("\t")[1]:
            friends = line.split("\t")[1]
            friends = friends.split(',')

            for friend in friends:
                if user < friend:
                    yield (user, friend), 1
                else:
                    yield (friend, user), 1


def reducer_friends(friends, counts):
    if status == "couple":
        if sum(counts) == 2:
            yield "Couple Friends", friends
    else:
        if sum(counts) == 1:
            yield "Single Friends", friends


class MapReduce(MRJob, ABC):
    def steps(self):
        return [
            MRStep(mapper=mapper_get_friends,
                   reducer=reducer_friends),
        ]


if __name__ == '__main__':
    MapReduce.run()
