module koalas.groupby;

import koalas.util;
import koalas.dataframe;

import std.meta: AliasSeq;
import std.traits    : isCallable, PIT=ParameterIdentifierTuple, PTT=ParameterTypeTuple;

struct GroupbyItem(IT,RT)
{
    IT index;
    Dataframe!RT items;

}

struct Groupby(IT,RT)
{
    GroupbyItem!(IT,RT)[] groups;

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