import luigi
import luigi.util
import csv

from pathlib import Path
from hashlib import blake2s
from .util import data_out_path, Config, destination

from .extract_data import ExtractData
from .download_person import DownloadPerson

@luigi.util.requires(ExtractData)
class DownloadPeople(luigi.Task):
    task_namespace = 'voxceleb'

    start_at = luigi.IntParameter(default=0)
    stop_at = luigi.IntParameter(default=99999)

    def output(self):
        return luigi.LocalTarget(data_out_path('processed', 'DownloadedPeople', 'max_people_{}.dummy'.format(self.stop_at)))

    def run(self):
        people = self._get_people()
        people = people[self.start_at:self.stop_at]
        
        people_tasks = [DownloadPerson(person=p) for p in people]
        yield people_tasks
        with self.output().open("w") as f:
            f.write("done")

    def _get_people(self):
        with open('{}/list1.csv'.format(destination())) as csvfile:
            readCSV = csv.reader(csvfile, delimiter=',')
            people = []
            for row in readCSV:
                people.append(row[0])
        return people