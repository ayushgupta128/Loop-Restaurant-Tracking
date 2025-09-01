from setuptools import setup, find_packages
setup(
    name = 'pipe',
    version = '1.0',
    packages = find_packages(include = ('pipe*', )) + ['prophecy_config_instances.pipe'],
    package_dir = {'prophecy_config_instances.pipe' : 'configs/resources/pipe'},
    package_data = {'prophecy_config_instances.pipe' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.3'],
    entry_points = {
'console_scripts' : [
'main = pipe.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
