import os
import luigi
import luigi.util

from .util import data_out_path, Config

from .extract_data import ExtractData
from .download_video import DownloadVideo
from .process_video_for_person import ProcessVideoForPerson

from pathlib import Path


@luigi.util.requires(ExtractData)
class DownloadPerson(luigi.Task):
    task_namespace = 'voxceleb'

    priority = 1

    person = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(data_out_path('processed', 'DownloadedPerson', '{}.dummy'.format(self.person)))

    def run(self):
        person_path = Path(self.input().path, self.person)
        if(os.path.exists(person_path)):
            videos = [p.stem for p in person_path.iterdir()]
            dw_tasks = [DownloadVideo(video=v, person=self.person) for v in videos]
            yield dw_tasks
        
        with self.output().open("w") as f:
            f.write("Done")

        
