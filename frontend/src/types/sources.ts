export interface Source {
    name: string;
    type: string;
    description: string;
    frequency: string;
}

export interface SourceData {
    type: 'FeatureCollection';
    features: any[];
}

export interface MapSource {
    id: string;
    data: SourceData;
    color: string;
    opacity: number;
}
