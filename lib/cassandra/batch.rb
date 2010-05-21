class Cassandra
  class Batch
    attr_accessor :consistency

    def initialize
      @steps = []
    end

    def <<(mutation)
      if mutation[0] == :remove
        @steps << mutation
        @map = nil
      else
        unless @map
          @map = {}
          @steps << [@map]
        end
        update_mutation_map(mutation)
      end
      self
    end

    def update_mutation_map(mutation)
      this_map = mutation.first
      this_map.each do |key, column_families|
        map_row = @map[key] ||= {}
        column_families.each do |column_family, operations|
          cf_map = map_row[column_family] ||= {}
          operations.each do |operation|
            if operation.deletion
              if operation.deletion.super_column
                process_super_deletion(cf_map[:super] ||= {}, operation)
              else
                process_simple_deletion(cf_map[:simple] ||= {}, operation)
              end
            elsif operation.column_or_supercolumn.super_column.nil?
              process_simple_mutation(cf_map[:simple] ||= {}, operation)
            else
              process_super_mutation(cf_map[:super] ||= {}, operation)
            end
          end
        end
      end
    end

    def process_simple_mutation(container, operation)
      mutation = operation.column_or_supercolumn.column
      column_ops = container[mutation.name] ||= {}
      clean_simple(mutation.name, column_ops[mutation.timestamp])
      column_ops[mutation.timestamp] = operation
    end

    def process_simple_deletion(container, operation)
      mutation = operation.deletion
      mutation.predicate.column_names.each do |col|
        column_ops = container[col] ||= {}
        clean_simple(col, column_ops[mutation.timestamp])
        column_ops[mutation.timestamp] = operation
      end
    end

    def process_super_mutation(container, operation)
      mutation = operation.column_or_supercolumn.super_column
      supercol_ops = container[mutation.name] ||= {}
      mutation.columns.each do |col|
        col_ops = supercol_ops[col.name] ||= {}
        clean_super(col.name, col.timestamp, col_ops[col.timestamp])
        col_ops[col.timestamp] = operation
      end
    end

    def process_super_deletion(container, operation)
      mutation = operation.deletion
      supercol_ops = container[mutation.super_column] ||= {}
      mutation.predicate.column_names.each do |col|
        col_ops = supercol_ops[col] ||= {}
        clean_super(col, mutation.timestamp, col_ops[mutation.timestamp])
        col_ops[mutation.timestamp] = operation
      end
    end

    def clean_simple(name, orig_operation)
      return unless orig_operation
      if del = orig_operation.deletion and del.column_names.size > 1
        del.column_names.reject! {|col| col == name}
      end
    end

    def clean_super(name, timestamp, orig_operation)
      return unless orig_operation
      if del = orig_operation.deletion and del.column_names.size > 1
        del.column_names.reject! {|col| col == name}
      elsif cols = orig_operation.column_or_supercolumn.super_column.columns and cols.size > 1
        cols.reject! {|col| col.name == name and col.timestamp == timestamp}
      end
    end

    def reduced_steps
      @steps.collect do |step|
        case step.first
        when :remove
          step
        else
          set = [reduce_step(step[0])]
          set << consistency if consistency
          set
        end
      end
    end

    def reduce_step(step)
      step.inject({}) do |map, rows|
        map[rows[0]] = rows[1].inject({}) do |row, families|
          operations = families[1][:simple] ? reduce_simple(families[1][:simple]) : reduce_super(families[1][:super])
          row[families[0]] = operations.uniq
          row
        end
        map
      end
    end

    def reduce_simple(columns)
      (columns.values.collect {|timestamps| timestamps.values}).flatten
    end

    def reduce_super(supers)
      (supers.values.collect {|columns| reduce_simple(columns) }).flatten
    end

    def each
      steps = reduced_steps
      steps.each {|step| yield(step)}
    end
  end
end
