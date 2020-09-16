module koalas.groupby;

import koalas.util;
import koalas.dataframe;

import std.meta: AliasSeq;
import std.traits: isNumeric;
import std.algorithm: sum, mean, maxElement, minElement;

struct GroupbyItem(IT,RT)
{
    IT index;
    Dataframe!RT items;

}

struct Groupby(IT,RT)
{
    GroupbyItem!(IT,RT)[] groups;

    alias record_type = RT;
    alias idx_type = IT;

    alias memberTypes = AliasSeq!(typeof(RT.tupleof));
    alias memberNames = AliasSeq!(RT.tupleof);

    alias idxMemberTypes = AliasSeq!(typeof(IT.tupleof));
    alias idxMemberNames = AliasSeq!(IT.tupleof);

    auto count(){
        alias indexNames = AliasSeq!(IT.tupleof);

        enum new_type_name = RT.stringof~"_"~IT.stringof~"_"~"Agg";
        mixin(GenAddStructString!(IT,new_type_name,"ulong count;"));
        mixin("alias new_type = "~new_type_name~";");
        Dataframe!new_type df;
        foreach(group;groups){
            new_type row;
            static foreach (i,name; indexNames){
                mixin("row."~name.stringof~"=group.index."~name.stringof~";");
            }
            row.count = group.items.records.length;
            df.records ~= row;
        }
        return df;
    }

    auto first(){
        Dataframe!RT new_df;
        foreach(group;groups){
            if(group.items.length > 0)
                new_df.records ~= group.items.records[0];
        }
        return new_df;
    }

    auto numericApply(string fun)(){
        mixin(GenerateSubsetNumericDataframe(Groupby!(IT,RT)()));
        df.length = groups.length;
        foreach (i,group; groups)
        {
            static foreach (j,member; idxMemberNames)
            {
                mixin("df.records[i]."~member.stringof~" = group.index."~member.stringof~";");
            }
            static foreach (j,member; memberNames[idxMemberNames.length..$])
            {
                static if(isNumeric!(memberTypes[idxMemberNames.length+j]))
                    mixin("df.records[i]."~member.stringof~" = group.items."~member.stringof~"."~fun~";");
            }
        }
        return df;
    }

    // alias sum = numericApply!"sum";

    // auto mean(){
    //     mixin(GenerateSubsetDoubleDataframe(Groupby!(IT,RT)()));
    //     df.length = groups.length;
    //     foreach (i,group; groups)
    //     {
    //         static foreach (j,member; idxMemberNames)
    //         {
    //             mixin("df.records[i]."~member.stringof~" = group.index."~member.stringof~";");
    //         }
    //         static foreach (j,member; memberNames[idxMemberNames.length..$])
    //         {
    //             static if(isNumeric!(memberTypes[idxMemberNames.length+j]))
    //                 mixin("df.records[i]."~member.stringof~" = group.items."~member.stringof~".mean;");
    //         }
    //     }
    //     return df;
    // }

    // alias max = numericApply!"maxElement";

    // alias min = numericApply!"minElement";

    
    
    string toString(){
        import std.conv: to;
        string output;
        alias memberNames = AliasSeq!(IT.tupleof);
        foreach (GroupbyItem!(IT, RT) group; groups)
        {
            static foreach (member; memberNames){
                output~=member.stringof~": ";
                mixin("output~=group.index."~member.stringof~".to!string;");
                output~="\t";
            }
            output = output[0..$-1] ~'\n';
            output ~= group.items.toString;
        }
        return output;
    }
}

unittest{
    import std.stdio;
    auto df =  Dataframe().addNewCol!(string,"chrom").addNewCol!(int,"pos1").addNewCol!(double,"pos2");
    df.records~= df.record_type("1",2,0.2);
    df.records~= df.record_type("1",2,0.3);
    df.records~= df.record_type("2",3,0.4);
    df.records~= df.record_type("q",7,0.5);
    df.records~= df.record_type("q",6,0.6);
    df.groupby!(["chrom","pos1"]).sum.writeln;
    df.groupby!(["chrom"]).sum.writeln;
}
