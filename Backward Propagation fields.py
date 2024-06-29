# 1. Storage of data, Expressions, and dependencies
import redis
import json

# Connect to Redis
redis_con = redis.Redis(host='localhost', port=6379)

# Example data and dependencies
course_data = {
    'course:c1': {'B': 2, 'C': 3, 'A': None},
    'courserun:cr1': {'A1': 5, 'B1': 6, 'C2': 1, 'D1': 7, 'E': None, 'F': None, 'D': None}
}

# Store the data in Redis
for key, value in course_data.items():
    redis_con.set(key, json.dumps(value))

# Store expressions for each field
expressions = {
    'course:c1:A': 'course:c1:B + course:c1:C * courserun:cr1:D',
    'courserun:cr1:D': 'courserun:cr1:E + courserun:cr1:F',
    'courserun:cr1:F': 'courserun:cr1:A1 + courserun:cr1:B1 if courserun:cr1:C2 > 0 else 0',
    'courserun:cr1:E': 'courserun:cr1:A1 * courserun:cr1:D1'
}

for field, expression in expressions.items():
    redis_con.set(f'expression:{field}', expression)

# Store dependencies
dependencies = {
    'course:c1:A': ['course:c1:B', 'course:c1:C', 'courserun:cr1:D'],
    'courserun:cr1:D': ['courserun:cr1:E', 'courserun:cr1:F'],
    'courserun:cr1:F': ['courserun:cr1:A1', 'courserun:cr1:B1', 'courserun:cr1:C2'],
    'courserun:cr1:E': ['courserun:cr1:A1', 'courserun:cr1:D1']
}

for field, deps in dependencies.items():
    redis_con.sadd(f'dependencies:{field}', *deps)



# 2. Calculation Engine with Expression Evaluation

def evaluate_expression(expression, data):
    try:
        return eval(expression, {}, data)
    except Exception as e:
        print(f"Error evaluating expression: {e}")
        return None


def calculate_field(entity_field):
    expression = redis_con.get(f'expression:{entity_field}')
    if not expression:
        return None

    # Prepare data context for evaluation
    data = {}
    dependencies = redis_con.smembers(f'dependencies:{entity_field}')
    for dep in dependencies:
        entity, field = dep.decode().split(':', 1)
        entity_data = json.loads(redis_con.get(entity))
        data[field] = entity_data[field]

    # Evaluate the expression
    value = evaluate_expression(expression.decode(), data)
    if value is not None:
        entity, field = entity_field.split(':', 1)
        entity_data = json.loads(redis_con.get(entity))
        entity_data[field] = value
        redis_con.set(entity, json.dumps(entity_data))

    return value


# Example usage
calculate_field('course:c1:A')
calculate_field('courserun:cr1:D')
calculate_field('courserun:cr1:E')
calculate_field('courserun:cr1:F')



# 3. Backward Propagation


def back_propagate_update(entity_field):
    calculate_field(entity_field)
    dependent_keys = redis_con.scan_iter(f'dependencies:*')

    for dependent_key in dependent_keys:
        if entity_field in redis_con.smembers(dependent_key):
            field = dependent_key.decode().split(':', 1)[1]
            back_propagate_update(field)


# Example usage
back_propagate_update('courserun:cr1:D')
back_propagate_update('courserun:cr1:E')
