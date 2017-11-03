import logging
import os
import psutil

from collections import namedtuple
from utils import osinfo
from icrawl_plugin import IContainerCrawler
from utils.crawler_exceptions import CrawlError, CrawlUnsupportedPackageManager
from utils.dockerutils import (exec_dockerinspect,
                               get_docker_container_rootfs_path)
from utils.misc import join_abs_paths
from utils.namespace import run_as_another_namespace, ALL_NAMESPACES
from utils.features import PackageFeature
from utils.misc import subprocess_run

logger = logging.getLogger('crawlutils')

PackageFeature = namedtuple('PackageFeature', ['pkgname', 'pkgarchitecture'])

class ActivepackageContainerCrawler(IContainerCrawler):

    def get_feature(self):
        return 'activepackage'

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

        # Update for a different route.

        dbpath = os.path.join(root_dir, dbpath)

        #TODO: call subprocess.Popen instead and check for err, skip htose
        output = subprocess_run(['dpkg', '-S',
                                 '--admindir={0}'.format(dbpath),
                                 filename],
                                shell=False)
        pkg = output.strip('\n')
        if pkg:
            name = pkg.split(':')[0]
            arch = pkg.split(':')[1]
            #yield (name, PackageFeature(name, arch))
            return (name, PackageFeature(name, arch))

    #TODO: store only unique packages
    def get_dpkg_packages(
            self,
            root_dir='/',
            dbpath='var/lib/dpkg',
            installed_since=0,
            filelist=[]):
        for filename in filelist:
            yield self.get_dpkg_package(
                    root_dir,
                    dbpath,
                    installed_since,
                    filename)


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
        pdb.set_trace()
        logger.debug('Crawling Packages')

        pkg_manager = self._get_package_manager(root_dir)

        try:
            if pkg_manager == 'dpkg':
                dbpath = dbpath or 'var/lib/dpkg'
                for (key, feature) in self.get_dpkg_packages(
                        root_dir, dbpath, installed_since, filelist):
                    yield (key, feature, 'package')
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
