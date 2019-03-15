import os
import luigi

from .util import data_out_path, destination, Config, check_output
from .atomized_local_target import AtomizedLocalTarget

from pathlib import Path
import subprocess
import re
import time

class DownloadVideo(luigi.Task):
    task_namespace = 'voxceleb'

    priority = 4

    video = luigi.Parameter()
    person = luigi.Parameter()
    
    def videooutput(self):
        return luigi.LocalTarget(data_out_path('video', '{0}/{1}'.format(self.person, self.video)))
    
    def output(self):
        return luigi.LocalTarget(
            data_out_path('processed', 'DownloadedVideo', '{}'.format(self.person), '{}.dummy'.format(self.video)))

    def run(self):
        with AtomizedLocalTarget(self.videooutput()) as target:
            cmd = [Config().youtube_dl_bin, "--output", str(target.path)]
            cmd += [self.video]
            print(cmd)
            run = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if run.returncode != 0:
                target.fail()
                error = run.stderr.decode("utf-8")

                os.rename(destination('data/{}/{}'.format(self.person, self.video)), data_out_path('novideo/{}/{}'.format(self.person, self.video)))
                print(time.ctime(time.time()))

                if re.search("YouTube said:", error):
                    self.fail_softly(error)
                    return
                else:
                    raise RuntimeError("youtube-dl failed:\n\n{}\n\n{}".format(
                        run.stdout.decode("utf-8"),
                        run.stderr.decode("utf-8")
                    ))
                
            else:
                print(time.ctime(time.time()))
                with self.output().open("w") as f:
                    f.write("done")        

        # check_output(self.output().path)