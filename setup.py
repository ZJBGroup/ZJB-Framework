import setuptools

setuptools.setup(
    name="zjb-framework",
    version="0.2.5",
    description="The underlying framework of Zhejiang Lab's digital twin brain platform",
    author="ZJB Group",
    url="https://github.com/ZJBGroup/ZJB-Framework",
    install_requires=["traits>=6.4.1", "ulid-py"],
    packages=setuptools.find_packages(),
)
