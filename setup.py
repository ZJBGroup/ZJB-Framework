import setuptools

setuptools.setup(
    name='zjb-framework',
    version='0.2.0',
    install_requires=[
        'traits>=6.4.1',
        'ulid-py'
    ],
    packages=setuptools.find_packages(),
)
