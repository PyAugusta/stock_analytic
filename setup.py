from setuptools import setup

def read_reqs(fn):
    with open(fn, 'r') as f:
        reqs = [lib for lib in f.readlines() if lib]
    return reqs

setup(
    name='my stock analytic',
    author='Cory Taylor',
    install_requires=read_reqs('requirements.txt'),
    package_dir={'analytic': 'analytic'},
    package_data={'analytic': ['analytic/assets/*']}
)
