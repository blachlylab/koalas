# koalas

A WIP dlang dataframe. Built using mainly compile time features (magic). You must know the size and type of your data coming in.

### Some Examples
Create a struct with your column names and types:
```
struct Row
{
	string chrom;
    int pos;
    string refAllele;
}
```

Pull in the data from a table:
```
import koalas;
struct Row
{
	string chrom;
    int pos;
    string refAllele;
}
void main(){
    Dataframe!Row df;
    // 0 is for skipping index columns
    // 1 is for skipping header row
    df.fromTable("file.tsv","\t",0,1);

    // is a groupby
    auto gby = df.groupby!(["chrom","pos"]);

    // Dataframe of aggregated counts
    auto counts = gby.count();
}
```

