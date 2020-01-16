__author__ = "Vivek Balasubramanian <vivek.balasubramanian@rutgers.edu>"
__copyright__ = "Copyright 2017, http://radical.rutgers.edu"
__license__ = "MIT"


class CalcError(Exception):
    """CalcError is the base exception raised by Ensemble Toolkit"""

    def __init__(self, msg):
        super(CalcError, self).__init__(msg)


class CalcTypeError(CalcError):
    """TypeError is raised if value of a wrong type is passed to a function or
    assigned as an attribute of an object"""

    def __init__(self, expected_type, actual_type, entity=None):

        if entity:
            msg = "Entity: %s, Expected (base) type(s) %s, but got %s." % (
                str(entity),
                str(expected_type),
                str(actual_type)
            )
        else:
            msg = "Expected (base) type(s) %s, but got %s." % (
                str(expected_type),
                str(actual_type)
            )
        super(CalcTypeError, self).__init__(msg)


class CalcValueError(CalcError):

    """
    ValueError is raised if a value that is unacceptable is passed to a
    function or assigned as an attribute of an object"""

    def __init__(self, obj, attribute, expected_value, actual_value):
        if type(expected_value) != list:
            msg = "Value for attribute %s of object %s incorrect. Expected value %s, but got %s." % (
                str(obj),
                str(attribute),
                str(expected_value),
                str(actual_value)
            )
        else:
            text = ''
            for item in expected_value:
                text += str(item)

            msg = "Value for attribute %s of object %s incorrect. Expected values %s, but got %s." % (
                str(obj),
                str(attribute),
                str(text),
                str(actual_value)
            )

        super(CalcValueError, self).__init__(msg)

