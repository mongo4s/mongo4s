package mongo4s.repositories

import mongo4s.operations.Sort

final case class Page[E](
    sort: Sort[E] = Sort.empty[E],
    skip: Option[Int] = None,
    limit: Option[Int] = None,
):
  def sortedBy(value: Sort[E]): Page[E] = copy(sort = value)
  def skipping(n: Int): Page[E]         = copy(skip = Some(n))
  def taking(n: Int): Page[E]           = copy(limit = Some(n))

object Page:
  def all[E]: Page[E]                     = Page()
  def first[E](n: Int): Page[E]           = Page(limit = Some(n))
  def sortedBy[E](sort: Sort[E]): Page[E] = Page(sort = sort)
