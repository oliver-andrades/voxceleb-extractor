import luigi
import luigi.util
import csv

from pathlib import Path
from hashlib import blake2s
from .util import data_out_path, Config, destination

from .extract_data import ExtractData
from .process_person import ProcessPerson

@luigi.util.requires(ExtractData)
class ProcessPeople(luigi.Task):
    task_namespace = 'voxceleb'

    start_at = luigi.IntParameter(default=0)
    stop_at = luigi.IntParameter(default=99999)
    max_videos = luigi.IntParameter(default=999)
    max_segments = luigi.IntParameter(default=999)

    def output(self):
        return luigi.LocalTarget(data_out_path('processed', 'ProcessPeople', 'max_people_{}.dummy'.format(self.stop_at)))

    def run(self):
        people = self._get_people()
        people = people[self.start_at:self.stop_at]

        people_tasks = [ProcessPerson(person=p, max_videos=self.max_videos, max_segments=self.max_segments) for p in people]
        yield people_tasks
        with self.output().open("w") as f:
            f.write(str(people))

    def _get_people(self):
        with open('{}/list1.csv'.format(destination())) as csvfile:
            readCSV = csv.reader(csvfile, delimiter=',')
            people = []
            for row in readCSV:
                people.append(row[0])
        return people