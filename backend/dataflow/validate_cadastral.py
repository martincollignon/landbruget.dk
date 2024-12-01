import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging

class ValidateGeometriesOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--dataset')
        parser.add_argument('--input_bucket')
        parser.add_argument('--output_bucket')

class ReadParquetDoFn(beam.DoFn):
    def __init__(self, input_bucket):
        self.input_bucket = input_bucket
    
    def process(self, dataset):
        import geopandas as gpd
        path = f'gs://{self.input_bucket}/raw/{dataset}/current.parquet'
        logging.info(f'Reading from {path}')
        gdf = gpd.read_parquet(path)
        yield {'dataset': dataset, 'data': gdf}

class ValidateCadastralDoFn(beam.DoFn):
    def process(self, element):
        from shapely.geometry import Polygon, MultiPolygon
        gdf = element['data']
        dataset = element['dataset']
        
        # Fix invalid geometries
        invalid_mask = ~gdf.geometry.is_valid
        if invalid_mask.any():
            gdf.loc[invalid_mask, 'geometry'] = gdf.loc[invalid_mask, 'geometry'].apply(
                lambda geom: geom.buffer(0) if geom else None
            )
        
        # Cadastral-specific validation
        gdf = gdf[
            gdf.geometry.is_valid &
            gdf.geometry.apply(lambda x: isinstance(x, (Polygon, MultiPolygon))) &
            ~gdf.geometry.is_empty &
            gdf.geometry.apply(lambda x: 0.1 <= x.area <= 100_000_000)  # Area between 0.1m² and 100km²
        ]
        
        stats = {
            'total_rows': len(element['data']),
            'valid_geometries': len(gdf)
        }
        
        element['data'] = gdf
        element['stats'] = stats
        yield element

class WriteResultsDoFn(beam.DoFn):
    def __init__(self, output_bucket):
        self.output_bucket = output_bucket
    
    def process(self, element):
        import pandas as pd
        dataset = element['dataset']
        gdf = element['data']
        stats = element['stats']
        
        # Write parquet
        output_path = f'gs://{self.output_bucket}/validated/{dataset}/current.parquet'
        logging.info(f'Writing to {output_path}')
        gdf.to_parquet(
            output_path,
            compression='zstd',
            compression_level=3,
            index=False,
            row_group_size=100000
        )
        
        # Write stats
        stats_path = f'gs://{self.output_bucket}/validated/{dataset}/validation_stats.csv'
        pd.DataFrame([stats]).to_csv(stats_path, index=False)
        
        yield element

def run(argv=None):
    """Build and run the pipeline."""
    pipeline_options = PipelineOptions(argv)
    options = pipeline_options.view_as(ValidateGeometriesOptions)
    
    with beam.Pipeline(options=pipeline_options) as p:
        (p 
         | 'Create Dataset' >> beam.Create([options.dataset])
         | 'Read Parquet' >> beam.ParDo(ReadParquetDoFn(options.input_bucket))
         | 'Validate Geometries' >> beam.ParDo(ValidateCadastralDoFn())
         | 'Write Results' >> beam.ParDo(WriteResultsDoFn(options.output_bucket))
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run() 