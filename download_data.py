import luigi

from .util import data_out_path, check_output, Config
from .atomized_local_target import AtomizedLocalTarget

import subprocess


class DownloadData(luigi.Task):
    task_namespace = 'voxceleb'

    url = luigi.Parameter(default="http://www.robots.ox.ac.uk/~vgg/data/voxceleb/data/vox{0}_{1}_txt.zip".format(Config().dataset, Config().data))

    def output(self):
        return luigi.LocalTarget(data_out_path('data.zip'))

    def run(self):
        with AtomizedLocalTarget(self.output()) as target:
            run = subprocess.run(["curl", "-L", "-o", str(target.path), self.url], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if run.returncode != 0:
                target.fail()
                raise RuntimeError("curl failed:\n\n{}\n\n{}".format(run.stdout.decode("utf-8"), run.stderr.decode("utf-8")))

        check_output(self.output().path)
