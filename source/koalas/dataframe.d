module koalas.dataframe;
import koalas.groupby;
import koalas.util;

import std.algorithm: map, filter, multiSort, uniq;
import std.array: array;
import std.array: split, join;
import std.stdio;
import std.meta: AliasSeq;
import std.conv: to;
import std.traits: isSomeChar, isArray, isSomeString;
import std.typecons: tuple;

struct Dataframe(RT){
    RT[] records;

    alias record_type = RT;
    alias memberTypes = AliasSeq!(typeof(RT.tupleof));
    alias memberNames = AliasSeq!(RT.tupleof);

    /// Allow getting column as a property
    /// TODO: fix issue when a column name overlaps existing df function
    /// e.g. column name length and length function below
    /// for now we will prevent these names as column names from compiling
    static foreach(i,member;memberNames){
        static assert(member.stringof != "length");
        static assert(member.stringof != "shape");
        static assert(member.stringof != "copy");
        static assert(member.stringof != "columns");
        static assert(member.stringof != "sort");
        static assert(member.stringof != "head");
        static assert(member.stringof != "toString");
        static assert(member.stringof != "unique");
        static assert(member.stringof != "records");
        mixin("alias "~member.stringof~" = getCol!\""~member.stringof~"\";");
    }

    this(RT[] rows){
        this.records = rows;
    }

    /// get number of rows
    @property ulong length(){
        return records.length;  
    }

    /// set number of rows
    @property void length(ulong len){
        records.length = len;
    }

    /// get shape of dataframe
    @property auto shape(){
        return tuple(records.length,columns.length);
    }

    /// get a copy of the dataframe
    auto copy(){
        return Dataframe!RT(records.dup);
    }

    /// returns a dataframe of filtered records
    /// filters on col == value if provided cmpOp is ==
    /// similar to df[df["col"] == value] in pandas
    template select(string col, string cmpOp = "=="){
        auto select(E)(E val) if(__traits(hasMember, RT, col))
        {
            mixin("static assert(is(typeof(RT." ~ col ~") == E));"); 
            mixin("auto fun = (RT x) => x." ~ col ~" "~ cmpOp ~" val;");
            return Dataframe!RT(records.filter!fun.array);
        }
    }

    /// Returns an array of T items that are of the same
    /// type as the col type in question
    /// similar to df["col"] in pandas
    auto getCol(string col)() if(__traits(hasMember, RT, col))
    {
        mixin("alias T = typeof(RT." ~ col ~");");
        T[] ret = new T[records.length];
        foreach(i,rec; records){
            mixin("ret[i] = rec."~ col ~";");
        }
        return ret;
    }
    
    /// Assigns a column using an array or singluar type 
    /// similar to df["col"] == val in pandas
    template setCol(string col){
        void setCol(E)(E[] arr) if(__traits(hasMember, RT, col) && !isSomeChar!E)
        {
            mixin("static assert(is(typeof(RT." ~ col ~") == E));"); 
            if(arr.length > records.length) records.length = arr.length;
            foreach(i,ref rec; records){
                mixin("rec."~ col ~" = arr[i];");
            }
        }
        void setCol(E)(E val) if(__traits(hasMember, RT, col) && (!isArray!E || isSomeString!E))
        {
            mixin("static assert(is(typeof(RT." ~ col ~") == E));"); 
            foreach(i,ref rec; records){
                mixin("rec."~ col ~" = val;");
            }
        }
    }

    /// Adds a new column to dataframe (will be empty column)
    /// realistically creates a new DF type since we are statically typed
    auto addNewCol(T,string name)(){

        // make new type name
        enum new_type_name = RT.stringof~name;

        // generate type
        mixin(GenAddStructString!(RT,new_type_name,T.stringof~" "~name~";"));
        mixin("alias new_type = "~new_type_name~";");

        // make new dataframe and copy
        Dataframe!new_type newdf;
        newdf.records.length = this.records.length;
        alias memberNames = AliasSeq!(RT.tupleof);
        static foreach (memberName; memberNames)
        {
            newdf.setCol!(memberName.stringof)(this.getCol!(memberName.stringof)());
        }
        return newdf;
    }

    /// reads dataframe from a file
    /// again must predefine columns with struct
    /// similar to pd.read_table in pandas
    void fromTable(string fn, string sep = "\t",int indexCols = 0, int headerCols = 0){
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

    /// returns string[] of columns 
    /// similar to df.columns in pandas
    static string[] columns(){
        string[] ret;
        static foreach(m;memberNames){
            ret ~= m.stringof;
        }
        return ret;
    }

    /// Returns a groupby object grouped by a string[] of indices 
    /// similar to df.groupby(["col","col2"]) in pandas
    auto groupby(string[] indices)(){

        // generate index name
        enum idx_type_name = GenIndexName!(__traits(identifier,RT),indices);
        static foreach (index; indices)
        {
            static assert(__traits(hasMember, RT, index));
        }

        // generate index type
        mixin(GenSubset!(idx_type_name,indices,RT));
        mixin("alias idx_type = "~idx_type_name~";");

        // make groupby
        Groupby!(idx_type,RT) gby;
        if(records.length == 0) return gby;

        // copy data to indexes
        auto new_records = records.dup;
        idx_type[] indexes = new idx_type[new_records.length];
        foreach (i,rec; new_records)
        {
            static foreach(name; AliasSeq!(idx_type.tupleof)){
                mixin("indexes[i]."~name.stringof~"=rec."~name.stringof~";");
            }    
        }

        // sort indexes and records
        mixin(GenMultiSort!idx_type("new_records"));
        mixin(GenMultiSort!idx_type("indexes"));

        // assign records to groups
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

    /// sorts a dataframe based on a string[] of indices 
    void sort(string[] indices)(){

        // generate indexes
        enum idx_type_name = GenIndexName!(__traits(identifier,RT),indices);
        static foreach (index; indices)
        {
            static assert(__traits(hasMember, RT, index));
        }
        mixin(GenSubset!(idx_type_name,indices,RT));
        mixin("alias idx_type = "~idx_type_name~";");
        
        // sort based on indexes
        if(records.length == 0) return;
        mixin(GenMultiSort!idx_type("records"));
    }

    /// sorts a dataframe based on all columns 
    void sort(){
        if(records.length == 0) return;
        mixin(GenMultiSort!RT("records"));
    }

    /// returns a dataframe of first n records 
    /// similar to df.head() in pandas
    auto head(ulong numRows = 5){
        if(numRows > records.length) numRows = records.length;
        return Dataframe!RT(records[0..numRows].dup);
    }

    /// Converts dataframe to string for printing 
    string toString(){
        auto i = 0;
        string output = "\t"~columns.join("\t")~"\n";
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

    /// Returns a Dataframe of specific columns from current dataframe
    auto subset(string[] indices)(){

        // generate subset type name
        enum sub_type_name = GenIndexName!(__traits(identifier,RT),indices);
        static foreach (index; indices)
        {
            static assert(__traits(hasMember, RT, index));
        }

        // generate subset type
        mixin(GenSubset!(sub_type_name,indices,RT));
        mixin("alias sub_type = "~sub_type_name~";");

        // create new data frame and copy data
        Dataframe!(sub_type) sub;
        if(records.length == 0) return sub;
        sub.length = this.length;
        foreach (i,rec; this.records)
        {
            static foreach(name; AliasSeq!(sub_type.tupleof)){
                mixin("sub.records[i]."~name.stringof~"=rec."~name.stringof~";");
            }    
        }
        return sub;
    }

    /// sort and get unique records from dataframe
    auto unique(){
        auto tmp = this.copy;
        tmp.sort;
        return Dataframe!RT(this.records.uniq.array);
    }

    /// apply function to column elements
    /// returns an array of results
    auto apply(alias fun, string col)(){
        import std.functional : unaryFun, binaryFun;
        return map!fun(this.getCol!col).array;
    }

    /// apply function to column elements
    /// returns an array of results
    auto apply(string fun, string col)(){
        import std.functional : unaryFun;
        return map!(unaryFun!fun)(this.getCol!col).array;
    }

    /// apply function to all rows
    /// returns an array of results
    auto apply(alias fun)(){
        import std.functional : unaryFun, binaryFun;
        return map!fun(this.records).array;
    }

    /// apply function to all rows
    /// returns an array of results
    auto apply(string fun)(){
        import std.functional : unaryFun;
        return map!(unaryFun!fun)(this.records).array;
    }

    /// allow foreach on dataframe
    int opApply(int delegate(ref RT) dg) {
        int result = 0;

        foreach (rec; records) {
            result = dg(rec);

            if (result) {
                break;
            }
        }

        return result;
    }

    auto opIndex(bool[] indexes){
        RT[] newRecords;
        ulong[] newIndexes;
        foreach (i,idx; indexes)
        {
            if(idx) newIndexes ~= i;
        }
        foreach (ulong key; newIndexes)
        {
            newRecords ~= records[key];
        }
        return Dataframe!RT(newRecords);
    }
}


/// Base dataframe type
private struct __BaseDf{

}

/// Create empty dataframe
auto Dataframe(){
    return Dataframe!__BaseDf();
}

auto unique(T)(T[] arr){
    import std.algorithm: sort;
    return arr.sort.uniq.array;
}

auto concat(T)(Dataframe!T[] dfs ...){
    Dataframe!T master;
    foreach (df; dfs)
    {
        master.records ~= df.records;
    }
    return master;
}

private struct testRecord{
    string chrom;
    int pos;
    string other;
}

/// returns string[] of columns 
/// similar to df.columns in pandas
static string[] columns(RT)(RT row){
    alias memberNames = AliasSeq!(RT.tupleof);
    string[] ret;
    static foreach(m;memberNames){
        ret ~= m.stringof;
    }
    return ret;
}

unittest{
    import std.stdio;
    Dataframe!testRecord df;
    df.records~= df.record_type("1",2,"hi");
    df.records~= df.record_type("1",2,"his");
    df.records~= df.record_type("2",3,"high");
    df.records~= df.record_type("q",7,"no");
    df.records~= df.record_type("q",6,"no");
    assert(df.columns == ["chrom", "pos", "other"]);
    auto gby = df.groupby!(["chrom", "pos"]);
    assert(gby.count.getCol!"count" == [2, 1, 1, 1]);
    df.setCol!"other"("j");
    df.sort!(["chrom", "pos"]);
    df = concat(df,df);
    assert(df.apply!("a.to!string","pos") == ["2", "2", "3", "6", "7", "2", "2", "3", "6", "7"]);
    assert(df.apply!("a.pos * 2") == [4, 4, 6, 12, 14, 4, 4, 6, 12, 14]);
    auto sub = df.subset!(["chrom","pos"]);
    assert(sub.unique.getCol!"chrom" == ["1", "2", "q", "q", "1", "2", "q", "q"]);
    assert(sub.getCol!"chrom".unique == ["1","2","q"]);
    foreach(ref rec;sub){
        writeln(rec);
    }
    auto index = sub.apply!("a > 5","pos");
    assert(sub[index].pos == [6,7,6,7]);
    assert(sub.records[0].columns == sub.columns);
}

unittest{
    auto df = Dataframe();
    auto df2 =df.addNewCol!(int,"pos");
    auto df3 = df2.addNewCol!(int,"pos2");
}