import logging
import os
import psutil
import subprocess

from collections import namedtuple
from utils import osinfo
from icrawl_plugin import IContainerCrawler
from utils.crawler_exceptions import CrawlError, CrawlUnsupportedPackageManager
from utils.dockerutils import (exec_dockerinspect,
                               get_docker_container_rootfs_path)
from utils.misc import join_abs_paths
from utils.namespace import run_as_another_namespace, ALL_NAMESPACES

logger = logging.getLogger('crawlutils')

ActivePackageFeature = namedtuple('ActivePackageFeature', ['pkgname', 'pkgarchitecture'])

class ActivepackageContainerCrawler(IContainerCrawler):

    def get_feature(self):
        return 'activepackage'

    def subprocess_run(self, cmd, ignore_failure=True, shell=False):
        """
        Runs cmd_string as a shell command. It returns stdout as a string, and
        raises RuntimeError if the return code is not equal to `good_rc`.

        It returns the tuple: (stdout, stderr, returncode)
        Can raise AttributeError or RuntimeError:
        """
        try:
            proc = subprocess.Popen(
                cmd,
                shell=shell,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            out, err = proc.communicate()
            rc = proc.returncode

        except OSError as exc:
            raise RuntimeError('Failed to run ' + cmd + ': [Errno: %d] ' %
                               exc.errno + exc.strerror + ' [Exception: ' +
                               type(exc).__name__ + ']')
        if (not ignore_failure) and (rc != 0):
            raise RuntimeError('(%s) failed with rc=%s: %s' %
                               (cmd, rc, err))
        return (out, err)

    
    def get_dpkg_package(
            self,
            root_dir='/',
            dbpath='var/lib/dpkg',
            installed_since=0,
            filename=None):

        if filename is None:
           return

        if os.path.isabs(dbpath):
            logger.warning(
                'dbpath: ' +
                dbpath +
                ' is defined absolute. Ignoring prefix: ' +
                root_dir +
                '.')

        dbpath = os.path.join(root_dir, dbpath)

        (output, err) = self.subprocess_run(['dpkg', '-S',
                                 '--admindir={0}'.format(dbpath),
                                 filename],
                                shell=False)
        if not err:                        
            pkg = output.strip('\n')
            if pkg:
                name = pkg.split(':')[0]
                arch = pkg.split(':')[1]
                return (name, ActivePackageFeature(name, arch))

    #TODO: store only unique packages
    def get_dpkg_packages(
            self,
            root_dir='/',
            dbpath='var/lib/dpkg',
            installed_since=0,
            filelist=[]):
        for filename in filelist:
            pkg = self.get_dpkg_package(
                    root_dir,
                    dbpath,
                    installed_since,
                    filename)
            if pkg:
                yield pkg


    def _get_package_manager(self, root_dir):
        result = osinfo.get_osinfo(mount_point=root_dir)
        if result:
            os_distro = result['os']
        else:
            raise CrawlUnsupportedPackageManager()

        if os_distro in ['ubuntu', 'debian']:
            pkg_manager = 'dpkg'
        elif os.path.exists(os.path.join(root_dir, 'var/lib/dpkg')):
            pkg_manager = 'dpkg'
        else:
            pkg_manager = None
        return pkg_manager

    def crawl_activepackages(
            self,
            dbpath=None,
            root_dir='/',
            installed_since=0,
            reload_needed=True,
            filelist=[]):

        # package attributes: ["installed", "name", "size", "version"]

        import pdb
        #pdb.set_trace()
        logger.debug('Crawling Packages')

        pkg_manager = self._get_package_manager(root_dir)

        try:
            if pkg_manager == 'dpkg':
                dbpath = dbpath or 'var/lib/dpkg'
                for (key, feature) in self.get_dpkg_packages(
                        root_dir, dbpath, installed_since, filelist):
                    yield (key, feature, 'activepackage')
            else:
                logger.warning('Unsupported package manager for Linux distro')
        except Exception as e:
            logger.error('Error crawling packages',
                         exc_info=True)
            raise CrawlError(e)

    #TODO: store only unique files
    def _crawl_files(self):
        files = []
        for p in psutil.process_iter():
            for f in p.get_open_files():
                files.append(f.path)
            for mmap in p.memory_maps():
                mmap_path = getattr(mmap, 'path')
                if os.path.isabs(mmap_path):
                    files.append(mmap_path)
        return files

    def crawl(self, container_id=None, avoid_setns=False,
              root_dir='/', **kwargs):
        logger.debug('Crawling active packages for container %s' % container_id)
        inspect = exec_dockerinspect(container_id)
        state = inspect['State']
        pid = str(state['Pid'])
        
        get_packages_oob = kwargs.get('get_packages_oob', 'False')


        if avoid_setns:
            raise NotImplementedError()

        filelist = run_as_another_namespace(pid,
                                        ALL_NAMESPACES,
                                        self._crawl_files)
        if not filelist:
            return

        if get_packages_oob == 'True' :
            rootfs_dir = get_docker_container_rootfs_path(
                container_id)
            return self.crawl_activepackages(
                root_dir=join_abs_paths(rootfs_dir, root_dir),
                reload_needed=True,
                filelist=filelist)
        else:  # in all other cases, including wrong mode set
            return run_as_another_namespace(pid,
                                            ALL_NAMESPACES,
                                            self.crawl_activepackages,
                                            None,
                                            root_dir, 0, False,
                                            filelist)
