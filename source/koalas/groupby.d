module koalas.groupby;

import koalas.util;
import koalas.dataframe;

import std.meta: AliasSeq;
import std.traits: isNumeric;
import std.algorithm: sum, mean, maxElement, minElement, uniq;
import std.array : array;
import mir.ndslice;

/// Stores indexes to grouped items
struct GroupbyItem(IT)
{
    IT * index;
    size_t[] items;

}

/// Represents grouped data in a dataframe
struct Groupby(IT, string[] indices, Args...)
{
    alias DF = Dataframe!(Args);
    alias subset = subsetTuple!(DF.RT, indices, Args);

    DF * df;
    IT[] indexes;
    GroupbyItem!(IT)[] groups;

    this(DF * df, ulong[] idx) {
        
        this.df = df;
        if(df.records.length == 0) return;
        indexes = df.records.indexed(idx).map!subset.uniq.array;
        groups = indexes.map!((ref x) => GroupbyItem!IT(&x, [])).array;
        auto i = 0;
        
        foreach (row; idx)
        {
            if(indexes[i] != subset(df.records[row])) {
                i++;
                assert(indexes[i] == subset(df.records[row]));
            }
            groups[i].items ~= row;
        }
    }

    auto count(){
        alias CDF = Dataframe!(Zip!(IT.Types, IT.fieldNames), ulong, "count");
        CDF newDf;
        foreach(group;groups){
            
            CDF.RT row;
            static foreach (name; IT.fieldNames){
                mixin("row."~name~"=group.index."~name~";");
            }
            row.count = group.items.length;
            newDf.records ~= row;
        }
        return newDf;
    }

    auto first(){
        Dataframe!(Args) newDf;
        foreach(group;groups){
            if(group.items.length > 0)
                newDf.records ~= df.records.indexed(group.items[0..1]).front;
        }
        return newDf;
    }

    auto numericApply(string fun)(){
        alias NDF = Dataframe!(Zip!(IT.Types, IT.fieldNames), SubsetByTemplate!(isNumeric, IT, Args));
        NDF newDf; 
        newDf.length = groups.length;
        foreach (i,group; groups)
        {
            static foreach (member; IT.fieldNames)
            {
                mixin("newDf.records[i]."~member~" = group.index."~member~";");
            }
            static foreach (j,member; NDF.memberNames[IT.fieldNames.length..$])
            {
                mixin("newDf.records[i]."~member~" = df.records.indexed(group.items).member!\""~member~"\"."~fun~";");
            }
        }
        return newDf;
    }

    alias sum = numericApply!"sum";    
    
    string toString(){
        import std.conv: to;
        string output;
        foreach (GroupbyItem!(IT) group; groups)
        {
            static foreach (member; IT.fieldNames){
                output~=member~": ";
                mixin("output~=group.index."~member~".to!string;");
                output~="\t";
            }
            output = output[0..$-1] ~'\n';
            output ~= Dataframe!(Args)(df.records.indexed(group.items).ndarray).toString;
        }
        return output;
    }
}

unittest{
    import std.stdio;
    auto df =  Dataframe!(string,"chrom",int,"pos1",double,"pos2")();
    df.records~= df.recordType("1",2,0.2);
    df.records~= df.recordType("1",2,0.3);
    df.records~= df.recordType("2",3,0.4);
    df.records~= df.recordType("q",7,0.5);
    df.records~= df.recordType("q",6,0.6);
    df.records~= df.recordType("1",2,0.2);
    assert(df.groupby!("chrom","pos1").count.count == [3, 1, 1, 1]);
    assert(df.groupby!("chrom").sum.pos2 == [0.7, 0.4, 1.1]);
    assert(df.groupby!("chrom","pos1").sum.pos2 == [0.7, 0.4, 0.6, 0.5]);
}
