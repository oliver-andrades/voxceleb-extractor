import os
import luigi
import numpy
import zipfile

from .util import data_out_path, temp_dest, temp_out_path, Config, check_output
from .download_video import DownloadVideo
# from .soft_failure import softly_failing
from .atomized_local_target import AtomizedLocalTarget

from pathlib import Path
import time
import re

from python_speech_features import mfcc
from scipy.io import wavfile
from scipy.signal import resample
from matplotlib import pyplot
import numpy

# @softly_failing()
class ProcessSegment(luigi.Task):
	task_namespace = 'voxceleb'

	priority = 2

	path = luigi.Parameter()
	person = luigi.Parameter()
	video = luigi.Parameter()
	# video_path = luigi.Parameter()
	segment = luigi.Parameter()

	def requires(self):
		return DownloadVideo(video=self.video, person=self.person)

	# def output(self):
	# 	return luigi.LocalTarget(data_out_path('processed', 'ProcessedSegment', '{}'.format(self.person), '{}'.format(self.video), '{}.dummy'.format(self.segment)))
	def frameoutput(self):
		return luigi.LocalTarget(temp_out_path('f', '{}'.format(self.person), '{}/%d.bmp'.format(self.segment)))
	def croppedoutput(self):
		return luigi.LocalTarget(data_out_path('frame', '{}'.format(self.person), '{}/%d.jpg'.format(self.segment)))
	def output(self):
		return luigi.LocalTarget(data_out_path('zippedframes', '{}'.format(self.person), '{}.zip'.format(self.segment)))

	def run(self):
		(frames, start, stop) = self._get_frames()

		source = data_out_path('video', self.person)
        
		for filename in os.listdir(source):
			if filename.startswith(self.video):
				video_path = "{0}/{1}".format(source, filename)

		if len(frames) < 1000:
			with AtomizedLocalTarget(self.frameoutput()) as target:
				command = 'ffmpeg -y -i "{video}" -start_number 0 -vf fps=25 -ss {start} -to {stop} {target} -hide_banner'.format(video=video_path, start=start, stop=stop, target=target.path)
				os.system(command)
				print('################################################## Extraction Done #####################################################')

				# with AtomizedLocalTarget(self.croppedoutput()) as target1:
				source = Path(self.frameoutput().path).parent
				# target1 = target1.path
				crop = ''
				resize = ''
				delete = ''
				delete1 = ''
				for frame in os.listdir(source):
					i = int(frame.rstrip(".bmp"))
					crop = crop + 'ffmpeg -y -i {sdir}/{num}.bmp -vf "crop={w}*iw:{h}*ih:{x}*iw:{y}*ih" {sdir}/{num}.bmp\n'.format(sdir=source, num=i, w=frames[i][3], h=frames[i][4], x=frames[i][1], y=frames[i][2])
					resize = resize + 'convert {dir}/{num}.bmp -resize 96x128^ -gravity center -extent 96x128 {dir}/{num}.jpg\n'.format(dir=source, num=i)
					delete = delete + 'rm {dir}/{num}.bmp\n'.format(dir=source, num=i)
					delete1 = delete1 + 'rm {dir}/{num}.jpg\n'.format(dir=source, num=i)
				delete1 = delete1 + 'rmdir {}'.format(source)
				
				print(len(crop))
				print(len(resize))
				print(len(delete))

				if len(crop)<131072 and len(resize)<131072 and len(delete)<131072:
					retcrop = os.system(crop)
					print('#################################################### Cropping Done ####################################################')
					print(len(frames))
					
					os.system(resize)
					os.system(delete)
					print('#################################################### Resize Done ######################################################')

					with AtomizedLocalTarget(self.output()) as target2:
						zipf = zipfile.ZipFile(target2.path, 'w', zipfile.ZIP_DEFLATED)
						for root, dirs, files in os.walk(Path(target.path).parent):
							for file in files:
								zipf.write(os.path.join(root, file), file)
						zipf.close()

					os.system(delete1)
					print('#################################################### Zipping Done #####################################################')


					start = round(start - 0.16, 3)
					stop = round(stop + 0.20, 3)

					audio_dir = data_out_path('audio/{}'.format(self.person))
					os.makedirs(audio_dir, exist_ok=True)
					print(audio_dir)

					command = 'ffmpeg -y -i "{audio}" -ac 1 -ar 16000 -ss {start} -to {stop} {target}/{segment}.wav -hide_banner'.format(audio=video_path, start=start, stop=stop, target=audio_dir, segment=self.segment)
					os.system(command)
					print('################################################## Audio Extracted #####################################################')
					(rate, sig) = wavfile.read("{0}/{1}.wav".format(audio_dir, self.segment))
					mfcc_op = mfcc(sig, winstep=0.01, nfilt=40, lowfreq=300, highfreq=3700, samplerate=rate)
					numpy.save("{0}/{1}.npy".format(audio_dir, self.segment), mfcc_op)
					print('##################################################### MFCC Done ########################################################')

					# os.rename('{}/{}.txt'.format(self.path, self.segment), data_out_path('donedata', '{0}/{1}'.format(self.person, self.video), '{}.txt'.format(self.segment)))
					print(time.ctime(time.time()))

					# with self.output().open("w") as f:
					# 	f.write("done")

				else:
					print(len(frames))
					frame_dir = temp_dest('f', '{0}/{1}'.format(self.person, self.segment))
					target.fail()

					dead_dir = data_out_path('deaddata', '{0}/{1}'.format(self.person, self.video))
					os.makedirs(dead_dir, exist_ok=True)
					os.rename('{}/{}.txt'.format(self.path, self.segment), '{}/{}.txt'.format(dead_dir, self.segment))

					print('deleting framedir {}'.format(frame_dir))
					os.system('rm -rf {}'.format(frame_dir))
					failure = "################################################## Cropping Failed #####################################################"
					raise RuntimeError("\n{}\nCommand to long {}\n{}".format(failure, len(frames), time.ctime(time.time())))

		else:
				print(len(frames))
				frame_dir = temp_dest('f', '{0}/{1}'.format(self.person, self.segment))

				dead_dir = data_out_path('deaddata', '{0}/{1}'.format(self.person, self.video))
				os.makedirs(dead_dir, exist_ok=True)
				os.rename('{}/{}.txt'.format(self.path, self.segment), '{}/{}.txt'.format(dead_dir, self.segment))

				failure = "################################################## Cropping Failed #####################################################"
				raise RuntimeError("\n{}\nToo many Frames {}\n{}".format(failure, len(frames), time.ctime(time.time())))
				
		check_output(self.output().path)


	def _get_frames(self):
		vid_dir=self.path
		segment_file = Path("{0}/{1}.txt".format(self.path, self.segment))
		with segment_file.open('r') as f:
			segment_desc = f.read().strip()

		lines = segment_desc.split("\n")
		frames = numpy.array([i.split() for i in lines[7:]])
		start = (float(frames[0][0]) - 1)/ 25
		stop = (float(frames[-1][0]))/ 25

		return frames, start, stop