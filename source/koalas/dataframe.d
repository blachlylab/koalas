module koalas.dataframe;
import koalas.groupby;
import koalas.util;

import std.algorithm: map, filter, multiSort;
import std.array: array;
import std.array: split,join;
import std.stdio;
import std.meta: AliasSeq;
import std.conv: to;
import std.traits: isSomeChar;

struct Dataframe(RT){
    RT[] records;

    this(RT[] rows){
        this.records = rows;
    }

    template select(string col){
        auto select(E)(E val) if(__traits(hasMember, RT, col))
        {
            mixin("static assert(is(typeof(RT." ~ col ~") == E));"); 
            mixin("auto fun = (RT x) => x." ~ col ~" == val;");
            return Dataframe!RT(records.filter!fun.array);
        }
    }

    template invertedSelect(string col){
        auto select(E)(E val) if(__traits(hasMember, RT, col))
        {
            mixin("static assert(is(typeof(RT." ~ col ~") == E));"); 
            mixin("auto fun = (RT x) => x." ~ col ~" != val;");
            return Dataframe!RT(records.filter!fun.array);
        }
    }


    auto getCol(string col)() if(__traits(hasMember, RT, col))
    {
        mixin("alias T = typeof(RT." ~ col ~");");
        T[] ret = new T[records.length];
        foreach(i,rec; records){
            mixin("ret[i] = rec."~ col ~";");
        }
        return ret;
    }
    
    template setCol(string col){
        void setCol(E)(E[] arr) if(__traits(hasMember, RT, col) && !isSomeChar!E)
        {
            mixin("static assert(is(typeof(RT." ~ col ~") == E));"); 
            if(arr.length > records.length) records.length = arr.length;
            foreach(i,ref rec; records){
                mixin("rec."~ col ~" = arr[i];");
            }
        }
        void setCol(E)(E val) if(__traits(hasMember, RT, col))
        {
            mixin("static assert(is(typeof(RT." ~ col ~") == E));"); 
            foreach(i,ref rec; records){
                mixin("rec."~ col ~" = val;");
            }
        }
    }

    void fromTable(string fn, string sep = "\t",int indexCols = 0, int headerCols = 0){
        alias memberTypes = AliasSeq!(typeof(RT.tupleof));
        alias memberNames = AliasSeq!(RT.tupleof);
        auto file =  File(fn);
        string line;
        while(headerCols){
            file.readln();
            headerCols--;
        }
        while ((line = file.readln()) !is null){
            RT item;
            auto fields = line[0..$-1].split(sep);
            fields = fields[indexCols .. $];
            static foreach (i,member; memberTypes)
            {
                mixin("item."~memberNames[i].stringof~"=fields["~i.to!string~"].to!member;");
            }
            this.records ~= item;
        }
    }

    static string[] columns(){
        alias memberNames = AliasSeq!(RT.tupleof);
        string[] ret;
        static foreach(m;memberNames){
            ret ~= m.stringof;
        }
        return ret;
    }

    auto groupby(string[] indices)(){
        enum idx_type_name = GenIndexName!(__traits(identifier,RT),indices);
        static foreach (index; indices)
        {
            static assert(__traits(hasMember, RT, index));
        }
        mixin(GenSubset!(idx_type_name,indices,RT));
        mixin("alias idx_type = "~idx_type_name~";");
        Groupby!(idx_type,RT) gby;
        if(records.length == 0) return gby;
        auto new_records = records.dup;
        idx_type[] indexes = new idx_type[new_records.length];
        foreach (i,rec; new_records)
        {
            static foreach(name; AliasSeq!(idx_type.tupleof)){
                mixin("indexes[i]."~name.stringof~"=rec."~name.stringof~";");
            }    
        }
        mixin(GenMultiSort!idx_type("new_records"));
        GroupbyItem!(idx_type,RT) group;
        group.index = indexes[0];
        foreach (i,idx_type idx; indexes)
        {
            if(idx == group.index) group.items.records~=new_records[i];
            else{
                gby.groups~=group;
                group = group.init;
                group.index = idx;
                group.items.records~=new_records[i];
            }
        }
        if(group.items.records.length !=0) gby.groups~=group;
        return gby;
    }

    auto head(ulong numRows = 5){
        if(numRows > records.length) numRows = records.length;
        return Dataframe!RT(records[0..5].dup);
    }

    string toString(){
        auto i = 0;
        string output = "\t"~columns.join("\t")~"\n";
        alias memberNames = AliasSeq!(RT.tupleof);
        foreach (rec; records)
        {
            output~=i.to!string~"\t";
            static foreach (member; memberNames)
            {
                mixin("output~=rec."~member.stringof~".to!string;");
                output~="\t";
            }
            output = output[0..$-1] ~'\n';
            i++;
        }
        return output;
    }
}

private struct testRecord{
    string chrom;
    int pos;
    string other;
}

unittest{
    import std.stdio;
    Dataframe!testRecord df;
    df.records~= testRecord("1",2,"hi");
    df.records~= testRecord("1",2,"his");
    df.records~= testRecord("2",3,"high");
    df.records~= testRecord("q",7,"no");
    writeln(df.columns);
    auto gby = df.groupby!(["chrom", "pos"]);
    writeln(gby);
    writeln(gby.count);
    df.setCol!"other"("j");
    writeln(df);
}