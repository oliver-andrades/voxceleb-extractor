import luigi
import luigi.util

from .util import data_out_path, temp_dest, Config, check_output
from .download_video import DownloadVideo
from .process_segment import ProcessSegment
from .atomized_local_target import AtomizedLocalTarget

from pathlib import Path
import os
import subprocess
import re

class ProcessVideoForPerson(luigi.Task):
    task_namespace = 'voxceleb'
    priority = 4
    person = luigi.Parameter()
    video = luigi.Parameter()
    max_segments = luigi.IntParameter(default=999)

    def output(self):
        return luigi.LocalTarget(data_out_path('processed', 'ProcessVideoForPerson', '{}'.format(self.person), '{}_{}.dummy'.format(self.video, self.max_segments)))

    def run(self):
        (segments, vid_dir) = self._get_segments()
        segments = segments[:self.max_segments]

        processSegment_tasks = [ProcessSegment(path=str(vid_dir), person=self.person, video=self.video, segment=segment) for segment in segments]
        yield processSegment_tasks 

        source = data_out_path('video', self.person)
        if os.path.exists(source):
            for filename in os.listdir(source):
                if filename.startswith(self.video):
                    os.system('rm {0}/{1}'.format(source, filename))
            if not os.listdir('{0}'.format(source)):
                os.rmdir(source)

        #td = temp_dest('frames', '{}'.format(self.person), '{}'.format(self.video))
        #if os.path.exists(td):
        #    os.system('rm -rf {}'.format(td))


        if not os.listdir(vid_dir):
            os.rmdir(vid_dir)
        with self.output().open("w") as f:
            f.write("done")

    def _get_segments(self):
        video_dir = data_out_path('data', self.person, self.video)
        segments=[]
        for segment_file in os.listdir(video_dir):
            segments.append(segment_file.rstrip(".txt"))

        return segments, video_dir