# Copyright (C) 2018 DynApps <http://www.dynapps.be>
# @author Stefan Rijnhart <stefan.rijnhart@dynapps.nl>
# License AGPL-3.0 or later (http://www.gnu.org/licenses/agpl).
import logging
from ast import literal_eval
from time import sleep

from psycopg2 import ProgrammingError
from psycopg2.sql import SQL, Identifier

from odoo import _, api, fields, models
from odoo.exceptions import AccessError, UserError
from odoo.tools import ormcache

_logger = logging.getLogger(__name__)


class DatabaseLoadIndicator(models.Model):
    _name = "database.load.indicator"
    _description = "Database Load Indicator"
    _order = "sequence, name"

    @api.model
    def _default_sequence(self):
        max_seq = self.search([], limit=1, order="sequence desc")
        return (max_seq.sequence if max_seq else 0) + 1

    name = fields.Char(required=True)
    active = fields.Boolean(default=True)
    debug = fields.Boolean(
        default=False, help="Log the result of every call to this indicator"
    )
    sequence = fields.Integer(default=_default_sequence)
    function = fields.Char(
        required=True,
        help=(
            "The name of the function defined in the database that this "
            "indicator should call. The function has to pre-exist. Defining "
            "new functions from this form here is not supported."
        ),
    )
    sleep_before = fields.Integer(
        default=1,
        help=(
            "How long to sleep before every call (i.e. after queuing the last "
            "the last set of jobs."
        ),
    )
    sleep = fields.Integer(
        string="Sleep After",
        default=3,
        help=(
            "How long to sleep when database is overloaded (in addition to "
            '"Sleep Before"'
        ),
    )
    factor = fields.Float(
        default=-25,
        help=(
            "Multiplier before applying the resulting load indicator. If "
            "the result is a negative number, it is returned as 0."
        ),
    )
    arguments = fields.Char(default="()")
    definition = fields.Text(
        compute="_compute_definition",
        help=(
            "This is the definition as retrieved from the Odoo database. "
            "You can only update the definition in the database itself."
        ),
    )

    def _compute_definition(self):
        """
        Fetch the definition of the function that is configured on the load
        indicator.
        """
        if not self:
            return
        self.env.cr.execute(
            """ SELECT proname, pg_get_functiondef(oid)
            FROM pg_proc WHERE proname in %s """,
            (tuple(self.mapped("function")),),
        )
        defs = dict(self.env.cr.fetchall())
        for record in self:
            record.definition = defs.get(record.function)

    @api.model
    @ormcache()
    def _get_active_indicator(self):
        """
        Fetch the first active indicator configuration in the database, and
        return it in a way that is safe for caching.
        """
        record = self.search([], limit=1)
        if record:
            result = record.read()[0]
            result["arguments"] = literal_eval(record.arguments or "()")
            return result
        return False

    @api.model
    def create(self, vals):
        """ Clear the cache when creating a new record """
        self._get_active_indicator.clear_cache(self)
        return super(DatabaseLoadIndicator, self).create(vals)

    def write(self, vals):
        """ Clear the cache when modifying a record """
        self._get_active_indicator.clear_cache(self)
        return super().write(vals)

    def unlink(self):
        """ Clear the cache when deleting a record """
        self._get_active_indicator.clear_cache(self)
        return super().unlink()

    def try_call(self):
        if not self.env.user.has_group("base.group_system"):
            raise AccessError(_("Operation not allowed"))
        self.ensure_one()
        arguments = literal_eval(self.arguments or "()")
        sql = SQL("SELECT * FROM {0}({1})").format(
            Identifier(self.function), SQL(", ".join("%s" for i in arguments))
        )
        self.env.cr.execute(sql, tuple(arguments))
        res = self.env.cr.fetchone()
        if not res:
            raise UserError(_("Function did not return a result"))
        raise UserError(_("Function returned %s") % res[0])

    @api.model
    def _call(self, indicator, default=None):
        """Execute the function that determines if there is a high
        database load. A valid function is any function that returns at least
        one value. The value, multiplied by the indicator's factor determines
        the load capacity that can be consumed. The load capacity is then
        decreased with every started job's function load.

        A function can return a boolean value. This value can be negated by
        a factor below zero. So a return value False in combination with a
        factor of -20 means that 20 jobs of function load 1 can be run.

        If more than one value is returned, this will trigger a debug log
        statement with the full output."""
        sleep(indicator.get("sleep_before", 0))
        arguments = indicator["arguments"]
        sql = SQL("SELECT * FROM {0}({1})").format(
            Identifier(indicator["function"]), SQL(", ".join("%s" for i in arguments))
        )
        query = self.env.cr.mogrify(sql, tuple(arguments))
        try:
            with self.env.cr.savepoint():
                self.env.cr.execute(query)
                row = self.env.cr.fetchone()
        except ProgrammingError as e:
            _logger.warn('Load indicator query "%s" raises error "%s"', query, e)
            return default
        if row:
            res = row[0]

            # Negate a boolean value
            factor = indicator["factor"] or 1
            if factor < 0 and res in (True, False):
                res = not res
                factor = abs(factor)
            try:
                res = float(res)
            except ValueError:
                _logger.info('Cannot convert "%s" to float', res)
                return default
            res = max(0, factor * res)
            if indicator["debug"]:
                # Allow to log some info
                _logger.info(
                    'Load indicator query "%s" returned: %s, which is '
                    "multiplied to %s",
                    indicator["name"],
                    row,
                    res,
                )
            return res

        _logger.warn('No result from load indicator "%s"', indicator["name"])
        return default
