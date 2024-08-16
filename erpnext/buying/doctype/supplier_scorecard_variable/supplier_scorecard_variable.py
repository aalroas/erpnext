# Copyright (c) 2017, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt


import sys

import frappe
from frappe import _
from frappe.model.document import Document
from frappe.query_builder.functions import Sum
from frappe.utils import getdate


class VariablePathNotFound(frappe.ValidationError):
	pass


class SupplierScorecardVariable(Document):
	def validate(self):
		self.validate_path_exists()

	def validate_path_exists(self):
		if "." in self.path:
			try:
				from erpnext.buying.doctype.supplier_scorecard_period.supplier_scorecard_period import (
					import_string_path,
				)

				import_string_path(self.path)
			except AttributeError:
				frappe.throw(_("Could not find path for " + self.path), VariablePathNotFound)

		else:
			if not hasattr(sys.modules[__name__], self.path):
				frappe.throw(_("Could not find path for " + self.path), VariablePathNotFound)


def get_total_workdays(scorecard):
	"""Gets the number of days in this period"""
	delta = getdate(scorecard.end_date) - getdate(scorecard.start_date)
	return delta.days


def get_item_workdays(scorecard):
	"""Gets the number of days in this period"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)
	total_item_days = frappe.db.sql(
		"""
			SELECT
				SUM(DATEDIFF( %(end_date)s, po_item.schedule_date) * (po_item.qty))
			FROM
				`tabPurchase Order Item` po_item,
				`tabPurchase Order` po
			WHERE
				po.supplier = %(supplier)s
				AND po_item.received_qty < po_item.qty
				AND po_item.schedule_date BETWEEN %(start_date)s AND %(end_date)s
				AND po_item.parent = po.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not total_item_days:
		total_item_days = 0
	return total_item_days


def get_total_cost_of_shipments(scorecard):
	"""Gets the total cost of all shipments in the period (based on Purchase Orders)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				SUM(po_item.base_amount)
			FROM
				`tabPurchase Order Item` po_item,
				`tabPurchase Order` po
			WHERE
				po.supplier = %(supplier)s
				AND po_item.schedule_date BETWEEN %(start_date)s AND %(end_date)s
				AND po_item.docstatus = 1
				AND po_item.parent = po.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if data:
		return data
	else:
		return 0


def get_cost_of_delayed_shipments(scorecard):
	"""Gets the total cost of all delayed shipments in the period (based on Purchase Receipts - POs)"""
	return get_total_cost_of_shipments(scorecard) - get_cost_of_on_time_shipments(scorecard)


def get_cost_of_on_time_shipments(scorecard):
	"""Gets the total cost of all on_time shipments in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates

	total_delivered_on_time_costs = frappe.db.sql(
		"""
			SELECT
				SUM(pr_item.base_amount)
			FROM
				`tabPurchase Order Item` po_item,
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Order` po,
				`tabPurchase Receipt` pr
			WHERE
				po.supplier = %(supplier)s
				AND po_item.schedule_date BETWEEN %(start_date)s AND %(end_date)s
				AND po_item.schedule_date >= pr.posting_date
				AND pr_item.docstatus = 1
				AND pr_item.purchase_order_item = po_item.name
				AND po_item.parent = po.name
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if total_delivered_on_time_costs:
		return total_delivered_on_time_costs
	else:
		return 0


def get_total_days_late(scorecard):
	"""Gets the number of item days late in the period (based on Purchase Receipts vs POs)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)
	total_delivered_late_days = frappe.db.sql(
		"""
			SELECT
				SUM(DATEDIFF(pr.posting_date,po_item.schedule_date)* pr_item.qty)
			FROM
				`tabPurchase Order Item` po_item,
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Order` po,
				`tabPurchase Receipt` pr
			WHERE
				po.supplier = %(supplier)s
				AND po_item.schedule_date BETWEEN %(start_date)s AND %(end_date)s
				AND po_item.schedule_date < pr.posting_date
				AND pr_item.docstatus = 1
				AND pr_item.purchase_order_item = po_item.name
				AND po_item.parent = po.name
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]
	if not total_delivered_late_days:
		total_delivered_late_days = 0

	total_missed_late_days = frappe.db.sql(
		"""
			SELECT
				SUM(DATEDIFF( %(end_date)s, po_item.schedule_date) * (po_item.qty - po_item.received_qty))
			FROM
				`tabPurchase Order Item` po_item,
				`tabPurchase Order` po
			WHERE
				po.supplier = %(supplier)s
				AND po_item.received_qty < po_item.qty
				AND po_item.schedule_date BETWEEN %(start_date)s AND %(end_date)s
				AND po_item.parent = po.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not total_missed_late_days:
		total_missed_late_days = 0
	return total_missed_late_days + total_delivered_late_days


def get_on_time_shipments(scorecard):
	"""Gets the number of late shipments (counting each item) in the period (based on Purchase Receipts vs POs)"""

	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	total_items_delivered_on_time = frappe.db.sql(
		"""
			SELECT
				COUNT(pr_item.qty)
			FROM
				`tabPurchase Order Item` po_item,
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Order` po,
				`tabPurchase Receipt` pr
			WHERE
				po.supplier = %(supplier)s
				AND po_item.schedule_date BETWEEN %(start_date)s AND %(end_date)s
				AND po_item.schedule_date <= pr.posting_date
				AND po_item.qty = pr_item.qty
				AND pr_item.docstatus = 1
				AND pr_item.purchase_order_item = po_item.name
				AND po_item.parent = po.name
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not total_items_delivered_on_time:
		total_items_delivered_on_time = 0
	return total_items_delivered_on_time


def get_late_shipments(scorecard):
	"""Gets the number of late shipments (counting each item) in the period (based on Purchase Receipts vs POs)"""
	return get_total_shipments(scorecard) - get_on_time_shipments(scorecard)


def get_total_received(scorecard):
	"""Gets the total number of received shipments in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				COUNT(pr_item.base_amount)
			FROM
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Receipt` pr
			WHERE
				pr.supplier = %(supplier)s
				AND pr.posting_date BETWEEN %(start_date)s AND %(end_date)s
				AND pr_item.docstatus = 1
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_total_received_amount(scorecard):
	"""Gets the total amount (in company currency) received in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				SUM(pr_item.received_qty * pr_item.base_rate)
			FROM
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Receipt` pr
			WHERE
				pr.supplier = %(supplier)s
				AND pr.posting_date BETWEEN %(start_date)s AND %(end_date)s
				AND pr_item.docstatus = 1
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_total_received_items(scorecard):
	"""Gets the total number of received shipments in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				SUM(pr_item.received_qty)
			FROM
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Receipt` pr
			WHERE
				pr.supplier = %(supplier)s
				AND pr.posting_date BETWEEN %(start_date)s AND %(end_date)s
				AND pr_item.docstatus = 1
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_total_rejected_amount(scorecard):
	"""Gets the total amount (in company currency) rejected in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				SUM(pr_item.rejected_qty * pr_item.base_rate)
			FROM
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Receipt` pr
			WHERE
				pr.supplier = %(supplier)s
				AND pr.posting_date BETWEEN %(start_date)s AND %(end_date)s
				AND pr_item.docstatus = 1
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_total_rejected_items(scorecard):
	"""Gets the total number of rejected items in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				SUM(pr_item.rejected_qty)
			FROM
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Receipt` pr
			WHERE
				pr.supplier = %(supplier)s
				AND pr.posting_date BETWEEN %(start_date)s AND %(end_date)s
				AND pr_item.docstatus = 1
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_total_accepted_amount(scorecard):
	"""Gets the total amount (in company currency) accepted in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				SUM(pr_item.qty * pr_item.base_rate)
			FROM
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Receipt` pr
			WHERE
				pr.supplier = %(supplier)s
				AND pr.posting_date BETWEEN %(start_date)s AND %(end_date)s
				AND pr_item.docstatus = 1
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_total_accepted_items(scorecard):
	"""Gets the total number of rejected items in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				SUM(pr_item.qty)
			FROM
				`tabPurchase Receipt Item` pr_item,
				`tabPurchase Receipt` pr
			WHERE
				pr.supplier = %(supplier)s
				AND pr.posting_date BETWEEN %(start_date)s AND %(end_date)s
				AND pr_item.docstatus = 1
				AND pr_item.parent = pr.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_total_shipments(scorecard):
	"""Gets the total number of ordered shipments to arrive in the period (based on Purchase Receipts)"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				COUNT(po_item.base_amount)
			FROM
				`tabPurchase Order Item` po_item,
				`tabPurchase Order` po
			WHERE
				po.supplier = %(supplier)s
				AND po_item.schedule_date BETWEEN %(start_date)s AND %(end_date)s
				AND po_item.docstatus = 1
				AND po_item.parent = po.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_ordered_qty(scorecard):
	"""Returns the total number of ordered quantity (based on Purchase Orders)"""

	po = frappe.qb.DocType("Purchase Order")

	return (
		frappe.qb.from_(po)
		.select(Sum(po.total_qty))
		.where(
			(po.supplier == scorecard.supplier)
			& (po.docstatus == 1)
			& (po.transaction_date >= scorecard.get("start_date"))
			& (po.transaction_date <= scorecard.get("end_date"))
		)
	).run(as_list=True)[0][0] or 0


def get_rfq_total_number(scorecard):
	"""Gets the total number of RFQs sent to supplier"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				COUNT(rfq.name) as total_rfqs
			FROM
				`tabRequest for Quotation Item` rfq_item,
				`tabRequest for Quotation Supplier` rfq_sup,
				`tabRequest for Quotation` rfq
			WHERE
				rfq_sup.supplier = %(supplier)s
				AND rfq.transaction_date BETWEEN %(start_date)s AND %(end_date)s
				AND rfq_item.docstatus = 1
				AND rfq_item.parent = rfq.name
				AND rfq_sup.parent = rfq.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]

	if not data:
		data = 0
	return data


def get_rfq_total_items(scorecard):
	"""Gets the total number of RFQ items sent to supplier"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				COUNT(rfq_item.name) as total_rfqs
			FROM
				`tabRequest for Quotation Item` rfq_item,
				`tabRequest for Quotation Supplier` rfq_sup,
				`tabRequest for Quotation` rfq
			WHERE
				rfq_sup.supplier = %(supplier)s
				AND rfq.transaction_date BETWEEN %(start_date)s AND %(end_date)s
				AND rfq_item.docstatus = 1
				AND rfq_item.parent = rfq.name
				AND rfq_sup.parent = rfq.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]
	if not data:
		data = 0
	return data


def get_sq_total_number(scorecard):
	"""Gets the total number of RFQ items sent to supplier"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				COUNT(sq.name) as total_sqs
			FROM
				`tabRequest for Quotation Item` rfq_item,
				`tabSupplier Quotation Item` sq_item,
				`tabRequest for Quotation Supplier` rfq_sup,
				`tabRequest for Quotation` rfq,
				`tabSupplier Quotation` sq
			WHERE
				rfq_sup.supplier = %(supplier)s
				AND rfq.transaction_date BETWEEN %(start_date)s AND %(end_date)s
				AND sq_item.request_for_quotation_item = rfq_item.name
				AND sq_item.docstatus = 1
				AND rfq_item.docstatus = 1
				AND sq.supplier = %(supplier)s
				AND sq_item.parent = sq.name
				AND rfq_item.parent = rfq.name
				AND rfq_sup.parent = rfq.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]
	if not data:
		data = 0
	return data


def get_sq_total_items(scorecard):
	"""Gets the total number of RFQ items sent to supplier"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)

	# Look up all PO Items with delivery dates between our dates
	data = frappe.db.sql(
		"""
			SELECT
				COUNT(sq_item.name) as total_sqs
			FROM
				`tabRequest for Quotation Item` rfq_item,
				`tabSupplier Quotation Item` sq_item,
				`tabSupplier Quotation` sq,
				`tabRequest for Quotation Supplier` rfq_sup,
				`tabRequest for Quotation` rfq
			WHERE
				rfq_sup.supplier = %(supplier)s
				AND rfq.transaction_date BETWEEN %(start_date)s AND %(end_date)s
				AND sq_item.request_for_quotation_item = rfq_item.name
				AND sq_item.docstatus = 1
				AND sq.supplier = %(supplier)s
				AND sq_item.parent = sq.name
				AND rfq_item.docstatus = 1
				AND rfq_item.parent = rfq.name
				AND rfq_sup.parent = rfq.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]
	if not data:
		data = 0
	return data


def get_rfq_response_days(scorecard):
	"""Gets the total number of days it has taken a supplier to respond to rfqs in the period"""
	supplier = frappe.get_doc("Supplier", scorecard.supplier)
	total_sq_days = frappe.db.sql(
		"""
			SELECT
				SUM(DATEDIFF(sq.transaction_date, rfq.transaction_date))
			FROM
				`tabRequest for Quotation Item` rfq_item,
				`tabSupplier Quotation Item` sq_item,
				`tabSupplier Quotation` sq,
				`tabRequest for Quotation Supplier` rfq_sup,
				`tabRequest for Quotation` rfq
			WHERE
				rfq_sup.supplier = %(supplier)s
				AND rfq.transaction_date BETWEEN %(start_date)s AND %(end_date)s
				AND sq_item.request_for_quotation_item = rfq_item.name
				AND sq_item.docstatus = 1
				AND sq.supplier = %(supplier)s
				AND sq_item.parent = sq.name
				AND rfq_item.docstatus = 1
				AND rfq_item.parent = rfq.name
				AND rfq_sup.parent = rfq.name""",
		{"supplier": supplier.name, "start_date": scorecard.start_date, "end_date": scorecard.end_date},
		as_dict=0,
	)[0][0]
	if not total_sq_days:
		total_sq_days = 0

	return total_sq_days

def get_all_quality_score(scorecard):
	if not frappe.db.exists("Supplier Score", scorecard.supplier):
		frappe.throw(f"Supplier '{scorecard.supplier}' does not exist in Supplier Score List. Please select a created supplier")
	compliance_score, quality_score, tech_infra_score, financial_score, market_image_score = frappe.db.get_value(
	    "Supplier Score",
        scorecard.supplier,
        ["compliance_of_service", "quality_certificate", 
         "technological_infrastructure", "financial_capacity", 
         "market_image"]
    )

    # Default to 0 if any of the values are None (not entered)
	compliance_score = compliance_score or 0
	quality_score = quality_score or 0
	tech_infra_score = tech_infra_score or 0
	financial_score = financial_score or 0
	market_image_score = market_image_score or 0
	total_score = compliance_score + quality_score + tech_infra_score + financial_score + market_image_score

	return total_score

def get_all_delivery_score(scorecard):
    on_time_delivery, fast_response, shipping_document, capacity_adequacy = frappe.db.get_value(
        "Supplier Score",
        scorecard.supplier,
        ["on_time_delivery", "fast_response_to_urgent_requests", 
         "shipping_document", "capacity_adequacy"]
    )

    on_time_delivery = on_time_delivery or 0
    fast_response = fast_response or 0
    shipping_document = shipping_document or 0
    capacity_adequacy = capacity_adequacy or 0
    total_score = on_time_delivery + fast_response + shipping_document + capacity_adequacy

    return total_score

def get_all_price_score(scorecard):
    payment_terms, competitive_pricing, on_time_offer, revised_offer = frappe.db.get_value(
        "Supplier Score",
        scorecard.supplier,
        ["payment_terms", "competitive_pricing", 
         "on_time_offer", "revised_offer"]
    )

    payment_terms = payment_terms or 0
    competitive_pricing = competitive_pricing or 0
    on_time_offer = on_time_offer or 0
    revised_offer = revised_offer or 0
    total_score = payment_terms + competitive_pricing + on_time_offer + revised_offer

    return total_score

def get_all_customer_satisfaction_score(scorecard):
    num_complaints, approach_complaints, quick_solution, effective_solution = frappe.db.get_value(
        "Supplier Score",
        scorecard.supplier,
        ["number_of_complaints", "approach_to_complaints", 
         "quick_solution", "effective_solution"]
    )

    num_complaints = num_complaints or 0
    approach_complaints = approach_complaints or 0
    quick_solution = quick_solution or 0
    effective_solution = effective_solution or 0
    total_score = num_complaints + approach_complaints + quick_solution + effective_solution

    return total_score
