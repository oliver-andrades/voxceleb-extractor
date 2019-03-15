import os
import luigi
import luigi.util

from .util import data_out_path, temp_dest, Config

from .extract_data import ExtractData
from .download_video import DownloadVideo
from .process_video_for_person import ProcessVideoForPerson

from pathlib import Path

@luigi.util.requires(ExtractData)
class ProcessPerson(luigi.Task):
    task_namespace = 'voxceleb'

    priority = 1

    person = luigi.Parameter()
    max_videos = luigi.IntParameter(default=999)
    max_segments = luigi.IntParameter(default=999)

    def output(self):
        return luigi.LocalTarget(data_out_path('processed', 'ProcessPerson', '{}_{}.dummy'.format(self.person, self.max_videos)))
   
    def run(self):
        person_path = Path(self.input().path, self.person)
        if(os.path.exists(person_path)):
            videos = [p.stem for p in person_path.iterdir()]
            videos = videos[:self.max_videos]
            path = data_out_path('video')

            video_tasks = [ProcessVideoForPerson(person=self.person, video=v, max_segments=self.max_segments) for v in videos]
            yield video_tasks

            td = temp_dest('frames', '{}'.format(self.person))
            if os.path.exists('{}'.format(td)):
                os.system('rm -rf {}'.format(td))

        with self.output().open("w") as f:
            f.write("Done")