from setuptools import setup, find_packages
setup(
    name = 'p1',
    version = '1.0',
    packages = find_packages(include = ('p1*', )) + ['prophecy_config_instances.p1'],
    package_dir = {'prophecy_config_instances.p1' : 'configs/resources/p1'},
    package_data = {'prophecy_config_instances.p1' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.9'],
    entry_points = {
'console_scripts' : [
'main = p1.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
