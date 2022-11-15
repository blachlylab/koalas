module koalas.groupby;

import koalas.util;
import koalas.dataframe;

import std.meta: AliasSeq;
import std.traits: isNumeric;
import std.algorithm: sum, mean, maxElement, minElement;
import mir.ndslice;

/// Stores indexes to grouped items
struct GroupbyItem(IT)
{
    IT * index;
    size_t[] items;

}

/// Represents grouped data in a dataframe
struct Groupby(IT, Args...)
{
    Dataframe!(Args) * df;
    IT[] indexes;
    GroupbyItem!(IT)[] groups;

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
    df.groupby!("chrom","pos1").writeln;
    df.groupby!("chrom","pos1").sum.writeln;
    df.groupby!("chrom").sum.writeln;
}
