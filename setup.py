from setuptools import setup, find_packages

setup(
    name='storage_handler',  # Replace with your package name
    version='0.1.0',
    packages=find_packages(),
    description='Unified Storage Handler for interacting with various storage backends.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='OctiLab Devolopers, Octi Zhang',
    author_email='zzyoctopus@gmail.com',
    url='https://zoctipus.github.io/',
    install_requires=[
        'fsspec>=2023.6.0',
        's3fs>=2023.6.0',
        'gcsfs>=2023.6.0',
        'paramiko>=2.7.2',       # For SFTP support
        'filelock>=3.0.0',
        # Add other dependencies as needed
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires='>=3.8',
)
