try:
    from setuptools import Extension, setup
except ImportError:
    from distutils.core import Extension, setup

import os
import sys
import os.path
import platform

def read(name):
    return open(os.path.join(os.path.dirname(__file__), name)).read()

if "posix" not in os.name:
    print "Are you really running a posix compliant OS ?"
    print "Be posix compliant is mandatory"
    sys.exit(1)

library_dirs=['/usr/local/lib']
include_dirs=[]



setup(name='pykt',
        version="0.0.1",
        description="KyotoTycoon client for Python",
        long_description=read("README.rst"),
        keywords='KyotoTycoon',
        author='yutaka matsubara',
        author_email='yutaka.matsubara@gmail.com',
        url='https://github.com/mopemope/pykt',
        license='BSD',
        platforms='Linux',
        test_suite = 'nose.collector',        
        packages= ["pykt"],
        ext_modules = [
            Extension('pykt',
            sources=['pykt/pykt.c', 'pykt/db.c', 'pykt/bucket.c', 
                'pykt/http.c', 'pykt/rpc.c', 'pykt/rest.c', 'pykt/buffer.c',
                'pykt/http_parser.c', 'pykt/request.c', 'pykt/response.c',
                'pykt/util.c', 'pykt/tsv_parser.c', 'pykt/tsv.c',
                'pykt/cursor.c'],
            #include_dirs=include_dirs,
            #library_dirs=library_dirs,
            #libraries=["profiler"],
            #define_macros=[("DEVELOP",None)],
            )
        ],

        classifiers=[
            "Intended Audience :: Developers",
            'License :: OSI Approved :: BSD License',
            'Operating System :: POSIX :: Linux',
            'Programming Language :: C',
            "Topic :: Database :: Front-Ends",
            "Topic :: Software Development :: Libraries"
        ],
)


