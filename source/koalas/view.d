module koalas.view;
import koalas.dataframe;
import koalas.util;
import mir.ndslice;
import std.traits : ReturnType;

struct ApplyView(alias fun, Args...) {
    Dataframe!(Args) * df;

    alias apply this;

    auto apply() {
        return this.df.records.map!fun;
    }

    auto fuse() {
        alias RT = ReturnType!fun;
        return Dataframe!(Zip!(RT.Types,RT.fieldNames))(this.apply.fuse.ndarray);
    }
}

struct View(Args...) {
    Dataframe!(Args) * df;
    size_t[] rowIdx;

    /// Allow getting column as a property
    /// TODO: fix issue when a column name overlaps existing df function
    /// e.g. column name length and length function below
    /// for now we will prevent these names as column names from compiling
    static foreach(i,member;df.memberNames){
        static assert(member != "length");
        static assert(member != "shape");
        static assert(member != "copy");
        static assert(member != "columns");
        static assert(member != "sort");
        static assert(member != "head");
        static assert(member != "toString");
        static assert(member != "unique");
        static assert(member != "records");
        mixin("alias "~member~" = getCol!\""~member~"\";");
    }

    /// Returns an array of T items that are of the same
    /// type as the col type in question
    /// similar to df["col"] in pandas
    auto getCol(string col)() if(__traits(hasMember, df.RT, col))
    {
        return this.view.member!(col);
    }

    this(Dataframe!(Args) * df, size_t[] rowIdx) {
        this.df = df;
        this.rowIdx = rowIdx;
    }

    alias view this;

    auto view() {
        return this.df.records.sliced.indexed(rowIdx);
    }

    auto fuse() {
        return Dataframe!(Args)(this.view.fuse.ndarray);
    }
}