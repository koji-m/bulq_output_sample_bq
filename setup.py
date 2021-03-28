import setuptools


setuptools.setup(
    name='bulq_output_sample_bq',
    version='0.0.1',
    install_requires=[],
    packages=setuptools.find_packages(),
    entry_points={
        'bulq.plugins.output': [
            f'sample_bq = bulq_output_sample_bq.bulq_output_sample_bq:BulqOutputSampleBq',
        ],
    }
)

